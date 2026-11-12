"""Compare two Spark join plans on the same municipal workload."""
import argparse
import json
from pathlib import Path
from statistics import median
from time import perf_counter

from pyspark.sql import SparkSession, functions as F


def compare(spark, silver):
    data = spark.read.parquet(str(silver / "sectors")).where(F.col("municipality") == "3550308")
    weights = spark.read.schema("station_id long, sector string, overlap_m2 double").json(str(silver / "station_sectors.jsonl"))
    inputs = {"setores": data.count(), "pares": weights.count()}

    def workload(strategy):
        lookup = F.broadcast(weights) if strategy == "broadcast" else weights.hint("MERGE")
        return data.join(lookup, "sector").groupBy("station_id").agg(F.count("*").alias("pairs")).orderBy("station_id")

    strategies = ("sort_merge", "broadcast")
    for strategy in strategies:
        workload(strategy).collect()
    samples = {strategy: [] for strategy in strategies}
    expected = None
    for run in range(3):
        for strategy in strategies if run % 2 == 0 else reversed(strategies):
            frame = workload(strategy)
            start = perf_counter()
            rows = frame.collect()
            samples[strategy].append(round(perf_counter() - start, 3))
            if expected is None:
                expected = rows
            if rows != expected:
                raise AssertionError("Join strategies returned different results")
    return {
        "master": spark.sparkContext.master,
        "inputs": inputs,
        "output_rows": len(expected),
        "same_output": True,
        "plans": {
            strategy: {
                "seconds": samples[strategy],
                "median_seconds": median(samples[strategy]),
                "join": next(
                    row.strip() for row in workload(strategy)._jdf.queryExecution().executedPlan().toString().splitlines()
                    if "Join" in row
                ),
            } for strategy in strategies
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver", type=Path, default=Path("data/silver"))
    parser.add_argument("--output", type=Path, default=Path("data/join-benchmark.json"))
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("Humberto Join Benchmark").master("local[2]")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = compare(spark, args.silver.resolve())
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
