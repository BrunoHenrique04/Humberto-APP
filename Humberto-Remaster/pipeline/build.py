"""Build the Humberto Bronze, Silver and Gold layers from public sources."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
from time import perf_counter

import geopandas as gpd
import pandas as pd
from pyspark.sql import SparkSession, functions as F

from sources import SOURCES

CITY = "3550308"
METRICS = {
    "sidewalk_pct": "v05221_perc",
    "clear_sidewalk_pct": "v05225_perc",
    "lighting_pct": "v05212_perc",
}
PROJECTED_CRS = 31983


def unwrap_csv(source, output):
    """Normalize the publisher's CSV, which wraps each data row in a CSV cell."""
    with source.open(encoding="utf-8-sig", newline="") as raw, output.open(
        "w", encoding="utf-8", newline=""
    ) as destination:
        records = csv.reader(raw)
        writer = csv.writer(destination, lineterminator="\n")
        header = next(records)
        writer.writerow(header)
        for row in records:
            values = next(csv.reader(row)) if len(row) == 1 else row
            if len(values) != len(header):
                raise ValueError(f"Invalid IBGE row {records.line_num}")
            writer.writerow(values)


def build_geography(bronze, silver, radius):
    stations = gpd.GeoDataFrame(pd.concat([
        gpd.read_file(bronze / f"estacao_{mode}.geojson").assign(mode=mode)
        for mode in ("metro", "trem")
    ], ignore_index=True), crs=4326).rename(columns={
        "cd_identificador": "station_id",
        "cd_identificador_linha": "line_id",
        "nm_estacao_metro_trem": "name",
        "nm_linha_metro_trem": "line",
    })
    boundary = gpd.read_file(bronze / "distrito_municipal.geojson").geometry.make_valid().union_all()
    stations = stations[
        stations.geometry.intersects(boundary) & stations.tx_situacao_metro_trem.eq("OPERANDO")
    ].copy()
    if stations.empty or stations.station_id.duplicated().any():
        raise ValueError("Missing stations or duplicate GeoSampa station IDs")
    stations["longitude"] = stations.geometry.x
    stations["latitude"] = stations.geometry.y
    station_columns = ["station_id", "line_id", "name", "line", "mode", "latitude", "longitude"]
    stations[station_columns].to_json(silver / "stations.jsonl", orient="records", lines=True, force_ascii=False)

    sectors = gpd.read_file(
        bronze / "entorno.gpkg", columns=["cd_setor"],
        where="cd_setor BETWEEN 355030800000000 AND 355030899999999",
    ).to_crs(PROJECTED_CRS)
    sectors["sector"] = sectors.cd_setor.astype(str)
    sectors.geometry = sectors.geometry.make_valid()
    buffers = stations[["station_id", "geometry"]].to_crs(PROJECTED_CRS)
    buffers.geometry = buffers.buffer(radius)
    overlap = gpd.overlay(buffers, sectors[["sector", "geometry"]], how="intersection")
    overlap["overlap_m2"] = overlap.area
    overlap = overlap[overlap.overlap_m2 > 0]
    weights = overlap.groupby(["station_id", "sector"], as_index=False).overlap_m2.sum()
    weights.to_json(silver / "station_sectors.jsonl", orient="records", lines=True)
    return len(stations), len(sectors), len(weights)


def manifest(bronze, radius):
    sources = []
    for name, url in sorted(SOURCES.items()):
        path = bronze / name
        with path.open("rb") as raw:
            digest = hashlib.file_digest(raw, "sha256").hexdigest()
        sources.append({"file": name, "bytes": path.stat().st_size, "sha256": digest, "url": url})
    digest = hashlib.sha256(json.dumps([radius, sources], sort_keys=True).encode()).hexdigest()
    return digest[:16], sources


def event_metrics_for_file(path):
    metrics = {"jobs": 0, "tasks": 0, "shuffle_read_bytes": 0, "shuffle_write_bytes": 0}
    with path.open() as stream:
        for line in stream:
            event = json.loads(line)
            if event["Event"] == "SparkListenerJobStart":
                metrics["jobs"] += 1
            if event["Event"] == "SparkListenerTaskEnd":
                metrics["tasks"] += 1
                task = event.get("Task Metrics") or {}
                read = task.get("Shuffle Read Metrics") or {}
                write = task.get("Shuffle Write Metrics") or {}
                metrics["shuffle_read_bytes"] += read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0)
                metrics["shuffle_write_bytes"] += write.get("Shuffle Bytes Written", 0)
    return metrics


def benchmark(data):
    result = {}
    for label, frame in (("Brasil", data), ("São Paulo", data.where(F.col("municipality") == CITY))):
        start = perf_counter()
        row = frame.agg(F.count("*").alias("rows"), F.count("clear_sidewalk_pct").alias("valid")).first()
        result[label] = {"seconds": round(perf_counter() - start, 3), **row.asDict()}
    return result


def run(args):
    bronze, root = args.bronze.resolve(), args.output.resolve()
    version, inputs = manifest(bronze, args.radius)
    root.mkdir(parents=True, exist_ok=True)
    silver = root / "silver"
    gold = root / "gold"
    events = root / "spark-events"
    for directory in (silver, gold, events):
        directory.mkdir(exist_ok=True)
    times = {}
    start = perf_counter()
    unwrap_csv(bronze / "entorno.csv", silver / "entorno_normalizado.csv")
    station_count, spatial_sector_count, overlap_count = build_geography(bronze, silver, args.radius)
    times["preparacao_e_juncao_espacial_s"] = round(perf_counter() - start, 3)

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = SparkSession.builder.appName("Humberto Medallion")
    if args.master:
        builder = builder.master(args.master)
    spark = (
        builder.config("spark.sql.shuffle.partitions", str(args.partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.rolling.enabled", "false")
        .config("spark.eventLog.compress", "false")
        .config("spark.eventLog.dir", events.as_uri())
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    app_id = spark.sparkContext.applicationId
    spark_info = {"version": spark.version, "master": spark.sparkContext.master, "configured_shuffle_partitions": args.partitions}
    try:
        start = perf_counter()
        raw = spark.read.option("header", True).option("mode", "FAILFAST").csv(str(silver / "entorno_normalizado.csv"))
        data = raw.select(
            F.col("cd_setor").alias("sector"),
            F.substring("cd_setor", 1, 7).alias("municipality"),
            *(F.expr(f"try_cast({source} as double)").alias(name) for name, source in METRICS.items()),
        )
        for field in METRICS:
            data = data.withColumn(field, F.when(F.col(field).between(0, 100), F.col(field)))
        data.write.mode("overwrite").parquet(str(silver / "sectors"))
        data = spark.read.parquet(str(silver / "sectors"))
        invalid_ids = data.where(~F.coalesce(F.col("sector").rlike(r"^[0-9]{15}$"), F.lit(False))).count()
        if invalid_ids or data.groupBy("sector").count().where(F.col("count") > 1).limit(1).count():
            raise ValueError("Invalid or duplicated census sector IDs")
        times["silver_s"] = round(perf_counter() - start, 3)
        workload = benchmark(data)
        start = perf_counter()
        station_schema = "station_id long, line_id long, name string, line string, mode string, latitude double, longitude double"
        overlap_schema = "station_id long, sector string, overlap_m2 double"
        stations = spark.read.schema(station_schema).json(str(silver / "stations.jsonl"))
        weights = spark.read.schema(overlap_schema).json(str(silver / "station_sectors.jsonl"))
        joined = weights.join(data.where(F.col("municipality") == CITY), "sector", "left")
        unmatched = joined.where(F.col("municipality").isNull()).count()
        grouped = joined.groupBy("station_id").agg(
            F.count("*").alias("sectors"),
            F.sum("overlap_m2").alias("covered_area_m2"),
            *(F.round(
                F.try_divide(F.sum(F.col(field) * F.col("overlap_m2")),
                             F.sum(F.when(F.col(field).isNotNull(), F.col("overlap_m2")))), 2
              ).alias(field) for field in METRICS),
            F.round(F.try_divide(F.sum(F.when(F.col("clear_sidewalk_pct").isNotNull(), F.col("overlap_m2"))),
                                 F.sum("overlap_m2")) * 100, 2).alias("valid_clear_area_pct"),
        )
        station_gold = stations.join(grouped, "station_id", "left").fillna(0, subset=["sectors"]).cache()
        line_gold = station_gold.groupBy("line_id", "line", "mode").agg(
            F.count("*").alias("stations"),
            F.count("clear_sidewalk_pct").alias("stations_with_environment_data"),
            F.round(F.avg("clear_sidewalk_pct"), 2).alias("mean_clear_sidewalk_pct"),
        )
        station_gold.write.mode("overwrite").parquet(str(gold / "stations"))
        line_gold.write.mode("overwrite").parquet(str(gold / "lines"))
        station_rows = [row.asDict() for row in station_gold.orderBy("station_id").collect()]
        line_rows = [row.asDict() for row in line_gold.orderBy("line_id").collect()]
        times["gold_s"] = round(perf_counter() - start, 3)
    finally:
        spark.stop()
    if unmatched:
        raise ValueError(f"{unmatched} spatial associations missing a sector in the IBGE CSV")
    execution_event = events / app_id
    report = {
        "version": version, "radius_m": args.radius, "spark": spark_info, "inputs": inputs,
        "counts": {"stations": station_count, "lines": len(line_rows), "city_geometry_sectors": spatial_sector_count,
                   "station_sector_pairs": overlap_count, "unmatched_pairs": unmatched,
                   "stations_with_clear_sidewalk_data": sum(row["clear_sidewalk_pct"] is not None for row in station_rows)},
        "timings": times, "workload": workload, "spark_metrics": event_metrics_for_file(execution_event),
    }
    snapshot = {"report": report, "stations": station_rows, "lines": line_rows}
    temporary = gold / "snapshot.tmp"
    temporary.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(gold / "snapshot.json")
    print(json.dumps({k: v for k, v in report.items() if k != "inputs"}, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze", type=Path, default=Path("data/bronze"))
    parser.add_argument("--output", type=Path, default=Path("data"))
    parser.add_argument("--radius", type=int, default=400)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--partitions", type=int, default=8)
    args = parser.parse_args()
    if args.radius <= 0 or args.partitions <= 0:
        parser.error("radius and partitions must be positive")
    run(args)


if __name__ == "__main__":
    main()
