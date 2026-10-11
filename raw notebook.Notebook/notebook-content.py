# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5fe20472-4ec1-444f-923f-6d9eda806d93",
# META       "default_lakehouse_name": "test",
# META       "default_lakehouse_workspace_id": "e14d575c-b79d-4255-b976-5437cdee9a69",
# META       "known_lakehouses": [
# META         {
# META           "id": "5fe20472-4ec1-444f-923f-6d9eda806d93"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# Cell 1: Parameters (a pipeline can pass these in)
# ============================================================
load_date = "2026-10-02"          # the one day to load (YYYY-MM-DD)
base_ccy  = "GBP"
quotes    = "EUR,USD,INR"

source_name = "frankfurter"
entity_name = "fx_rates"
api_url     = "https://api.frankfurter.dev/v2/rates"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Cell 2: Call the API for one day
# ============================================================
import requests, json
from datetime import datetime, timezone

params = {"date": load_date, "base": base_ccy, "quotes": quotes}

response = requests.get(api_url, params=params, timeout=60)
response.raise_for_status()          # stop if the API returns an error (4xx/5xx)

data = response.json()
print(f"Status: {response.status_code}, records: {len(data)}")
print(json.dumps(data, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Cell 3: Wrap with audit info (the API data itself is unchanged)
# ============================================================
raw_payload = {
    "_source":     source_name,
    "_entity":     entity_name,
    "_load_date":  load_date,
    "_loaded_at":  datetime.now(timezone.utc).isoformat(),
    "_request_url": response.url,
    "_record_count": len(data),
    "data": data                       # exact API response
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Cell 4: Save to the raw folder in the Lakehouse
# ============================================================
folder    = f"Files/raw/{source_name}/{entity_name}/load_date={load_date}"
file_path = f"{folder}/{entity_name}_{load_date}.json"

notebookutils.fs.mkdirs(folder)
notebookutils.fs.put(file_path, json.dumps(raw_payload, indent=2), True)  # True = overwrite

print(f"Saved: {file_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Cell 5: Check the file
# ============================================================
print(notebookutils.fs.head(file_path, 1000))      # first 1,000 characters

df_check = spark.read.option("multiline", "true").json(file_path)
df_check.selectExpr("_load_date", "_record_count", "explode(data) AS d") \
        .select("_load_date", "d.*")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
