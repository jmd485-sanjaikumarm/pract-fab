# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7885f1e4-36d2-4fb2-a570-f911f8c5aeeb",
# META       "default_lakehouse_name": "test",
# META       "default_lakehouse_workspace_id": "dffb1635-0edb-434c-84ad-9175952a3769",
# META       "known_lakehouses": [
# META         {
# META           "id": "7885f1e4-36d2-4fb2-a570-f911f8c5aeeb"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# ============================================================
# Cell 1: Parameters (toggle as parameter cell for pipelines)
# ============================================================
load_date   = "2026-10-02"        # one day; use "ALL" to load every raw file
source_name = "frankfurter"
entity_name = "fx_rates"
target_table = "bronze_fx_rates"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Cell 2: Read the raw JSON file(s)
# ============================================================
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, LongType, ArrayType)

base_path = f"Files/raw/{source_name}/{entity_name}"
path = base_path if load_date == "ALL" else f"{base_path}/load_date={load_date}"

# Fixed schema: safer than letting Spark guess the types
raw_schema = StructType([
    StructField("_source",        StringType()),
    StructField("_entity",        StringType()),
    StructField("_load_date",     StringType()),
    StructField("_loaded_at",     StringType()),
    StructField("_request_url",   StringType()),
    StructField("_record_count",  LongType()),
    StructField("data", ArrayType(StructType([
        StructField("date",  StringType()),
        StructField("base",  StringType()),
        StructField("quote", StringType()),
        StructField("rate",  DoubleType()),
    ])))
])

df_raw = (spark.read
          .option("multiline", "true")      # file is one pretty-printed JSON object
          .option("recursiveFileLookup", "true")
          .schema(raw_schema)
          .json(path)
          .withColumn("_file_name", F.input_file_name()))

print(f"Files read from: {path}")
df_raw.select("_load_date", "_record_count", "_file_name").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Cell 3: Flatten into one row per currency pair
# ============================================================
df_bronze = (df_raw
    .select(
        F.explode("data").alias("d"),
        "_source", "_load_date", "_loaded_at", "_file_name", "_record_count")
    .select(
        F.to_date("d.date").alias("rate_date"),
        F.col("d.base").alias("base_currency"),
        F.col("d.quote").alias("quote_currency"),
        F.col("d.rate").cast("decimal(18,6)").alias("fx_rate"),
        F.col("_source").alias("source"),
        F.to_date("_load_date").alias("load_date"),
        F.to_timestamp("_loaded_at").alias("api_called_at"),
        F.col("_file_name").alias("source_file"),
        F.col("_record_count").alias("expected_records"),
        F.current_timestamp().alias("ingested_at"))
)

df_bronze.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Cell 4: Checks before writing
# ============================================================
# 1. Rows in each file match the record count saved by the API notebook
check = (df_bronze.groupBy("load_date", "expected_records")
         .agg(F.count("*").alias("actual_records")))
bad = check.filter("expected_records <> actual_records").count()
if bad > 0:
    check.show()
    raise ValueError("Record count mismatch between raw file and flattened rows")

# 2. No empty key fields
nulls = df_bronze.filter(
    "rate_date IS NULL OR base_currency IS NULL OR "
    "quote_currency IS NULL OR fx_rate IS NULL").count()
if nulls > 0:
    raise ValueError(f"{nulls} rows have empty key fields")

# 3. No duplicate currency pairs per load date
dups = (df_bronze.groupBy("load_date", "rate_date", "base_currency", "quote_currency")
        .count().filter("count > 1").count())
if dups > 0:
    raise ValueError(f"{dups} duplicate currency pairs found")

print("All checks passed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Cell 4: Checks before writing
# ============================================================
# 1. Rows in each file match the record count saved by the API notebook
check = (df_bronze.groupBy("load_date", "expected_records")
         .agg(F.count("*").alias("actual_records")))
bad = check.filter("expected_records <> actual_records").count()
if bad > 0:
    check.show()
    raise ValueError("Record count mismatch between raw file and flattened rows")

# 2. No empty key fields
nulls = df_bronze.filter(
    "rate_date IS NULL OR base_currency IS NULL OR "
    "quote_currency IS NULL OR fx_rate IS NULL").count()
if nulls > 0:
    raise ValueError(f"{nulls} rows have empty key fields")

# 3. No duplicate currency pairs per load date
dups = (df_bronze.groupBy("load_date", "rate_date", "base_currency", "quote_currency")
        .count().filter("count > 1").count())
if dups > 0:
    raise ValueError(f"{dups} duplicate currency pairs found")

print("All checks passed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Cell 5: Write to the Delta table (rerun-safe)
# ============================================================
df_out = df_bronze.drop("expected_records")

if not spark.catalog.tableExists(target_table):
    # First run: create the table
    (df_out.write.format("delta")
        .partitionBy("load_date")
        .saveAsTable(target_table))
    print(f"Created table {target_table}")

elif load_date == "ALL":
    # Full reload: replace everything
    (df_out.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("load_date")
        .saveAsTable(target_table))
    print(f"Fully reloaded {target_table}")

else:
    # Normal daily run: replace only this day's rows
    (df_out.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"load_date = '{load_date}'")
        .saveAsTable(target_table))
    print(f"Replaced load_date = {load_date} in {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Cell 6: Verify the table
# ============================================================
spark.sql(f"""
    SELECT load_date, rate_date, base_currency, quote_currency, fx_rate, source_file
    FROM {target_table}
    ORDER BY load_date, quote_currency
""").show(truncate=False)

spark.sql(f"""
    SELECT load_date, COUNT(*) AS row_count
    FROM {target_table}
    GROUP BY load_date
    ORDER BY load_date
""").show()

# Delta history: each write is recorded (useful for audit / time travel)
spark.sql(f"DESCRIBE HISTORY {target_table}") \
     .select("version", "timestamp", "operation").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
