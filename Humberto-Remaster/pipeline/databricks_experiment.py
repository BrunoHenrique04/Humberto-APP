# Databricks notebook source
# MAGIC %md
# MAGIC # Humberto | Experimento com junção e agregação no Spark
# MAGIC Copie Silver/sectors e Silver/station_sectors.jsonl para um Volume.
# MAGIC Ajuste os parâmetros com caminhos nos quais o compute tem acesso.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("silver", "/Volumes/catalog/schema/volume/silver")
dbutils.widgets.text("output", "/Volumes/catalog/schema/volume/experiment")
silver = dbutils.widgets.get("silver").rstrip("/")
output = dbutils.widgets.get("output").rstrip("/")

# COMMAND ----------

sectors = spark.read.parquet(silver + "/sectors")
city = sectors.where(F.col("municipality") == "3550308")
weights = spark.read.schema("station_id long, sector string, overlap_m2 double").json(
    silver + "/station_sectors.jsonl"
)
matched = weights.join(city, "sector", "left")
assert matched.where(F.col("municipality").isNull()).count() == 0

# COMMAND ----------

valid_weight = F.sum(F.when(F.col("clear_sidewalk_pct").isNotNull(), F.col("overlap_m2")))
gold = matched.groupBy("station_id").agg(
    F.count("*").alias("sectors"),
    F.round(F.try_divide(F.sum(F.col("clear_sidewalk_pct") * F.col("overlap_m2")), valid_weight), 2)
        .alias("clear_sidewalk_pct"),
)
gold.explain("formatted")
gold.write.mode("overwrite").parquet(output + "/clear_sidewalk")
display(spark.read.parquet(output + "/clear_sidewalk").orderBy("clear_sidewalk_pct"))

# COMMAND ----------

# Registre Spark version, configuração do compute, jobs e shuffle na Spark UI.
# Compare station_id, sectors e clear_sidewalk_pct com a versão Gold local.
print({"spark_version": spark.version, "city_sectors": city.count(), "station_sector_pairs": weights.count()})
