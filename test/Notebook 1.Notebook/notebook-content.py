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

# PARAMETERS CELL ********************

# Leave all empty = automatic (catch-up) mode
load_date  = ""    # rerun ONE day,  e.g. "2026-09-25"
start_date = ""    # backfill FROM,  e.g. "2026-09-20"
end_date   = ""    # backfill TO,    e.g. "2026-09-24"
lag_days   = 5     # only load days at least this old

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests, json, time
from datetime import date, datetime, timedelta, timezone

API_URL = "https://archive-api.open-meteo.com/v1/archive"
CITY, LAT, LON = "Chennai", 13.08, 80.27
TABLE    = "bronze_weather"
RAW_PATH = "Files/raw/weather"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

latest_allowed = date.today() - timedelta(days=lag_days)

def days_between(start, end):
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]

if start_date and end_date:                         # 1. backfill a range
    mode  = "BACKFILL"
    dates = days_between(date.fromisoformat(start_date), date.fromisoformat(end_date))

elif load_date:                                     # 2. rerun one day
    mode  = "RERUN"
    dates = [date.fromisoformat(load_date)]

else:                                               # 3. automatic catch-up
    mode = "AUTO"
    if spark.catalog.tableExists(TABLE):
        last = spark.sql(f"SELECT MAX(weather_date) AS d FROM {TABLE}").first()["d"]
        start = last + timedelta(days=1)
    else:
        start = latest_allowed                      # first ever run
    dates = days_between(start, latest_allowed) if start <= latest_allowed else []

print(f"Mode: {mode}")
print(f"Dates to load: {[str(d) for d in dates]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def call_api(d, retries=3):
    params = {"latitude": LAT, "longitude": LON,
              "start_date": d, "end_date": d,
              "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
              "timezone": "auto"}
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(API_URL, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries:
                raise                                # give up → notebook fails
            print(f"  Attempt {attempt} failed: {e}. Retrying in 10s...")
            time.sleep(10)

def load_one_day(d):
    # 1. Call the API
    data = call_api(d)

    # 2. Save the raw response unchanged (one file per day; overwrite on rerun)
    notebookutils.fs.put(f"{RAW_PATH}/chennai_{d}.json", json.dumps(data), True)

    # 3. Pick out the values
    daily = data["daily"]
    row = (CITY,
           date.fromisoformat(daily["time"][0]),
           daily["temperature_2m_max"][0],
           daily["temperature_2m_min"][0],
           daily["precipitation_sum"][0],
           datetime.now(timezone.utc))
    df = spark.createDataFrame([row],
        "city string, weather_date date, temp_max double, temp_min double, "
        "rain_mm double, loaded_at timestamp")

    # 4. Replace this date: delete old rows, then insert
    if spark.catalog.tableExists(TABLE):
        spark.sql(f"DELETE FROM {TABLE} WHERE weather_date = DATE'{d}'")
    df.write.format("delta").mode("append").saveAsTable(TABLE)

    print(f"Loaded {d}: max {row[2]}°C, min {row[3]}°C, rain {row[4]} mm")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def call_api(d, retries=3):
    params = {"latitude": LAT, "longitude": LON,
              "start_date": d, "end_date": d,
              "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
              "timezone": "auto"}
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(API_URL, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries:
                raise                                # give up → notebook fails
            print(f"  Attempt {attempt} failed: {e}. Retrying in 10s...")
            time.sleep(10)

def load_one_day(d):
    # 1. Call the API
    data = call_api(d)

    # 2. Save the raw response unchanged (one file per day; overwrite on rerun)
    notebookutils.fs.put(f"{RAW_PATH}/chennai_{d}.json", json.dumps(data), True)

    # 3. Pick out the values
    daily = data["daily"]
    row = (CITY,
           date.fromisoformat(daily["time"][0]),
           daily["temperature_2m_max"][0],
           daily["temperature_2m_min"][0],
           daily["precipitation_sum"][0],
           datetime.now(timezone.utc))
    df = spark.createDataFrame([row],
        "city string, weather_date date, temp_max double, temp_min double, "
        "rain_mm double, loaded_at timestamp")

    # 4. Replace this date: delete old rows, then insert
    if spark.catalog.tableExists(TABLE):
        spark.sql(f"DELETE FROM {TABLE} WHERE weather_date = DATE'{d}'")
    df.write.format("delta").mode("append").saveAsTable(TABLE)

    print(f"Loaded {d}: max {row[2]}°C, min {row[3]}°C, rain {row[4]} mm")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for d in dates:
    load_one_day(str(d))      # each day is saved before the next starts

print(f"Finished. {len(dates)} day(s) loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Every date should have exactly 1 row
dups = spark.sql(f"""
    SELECT weather_date, COUNT(*) AS c FROM {TABLE}
    GROUP BY weather_date HAVING COUNT(*) > 1
""").count()
if dups > 0:
    raise ValueError(f"{dups} duplicate date(s) found")

# 2. Look for missing days between the first and last date
spark.sql(f"""
    SELECT MIN(weather_date) AS first_day,
           MAX(weather_date) AS last_day,
           COUNT(*)          AS days_loaded,
           DATEDIFF(MAX(weather_date), MIN(weather_date)) + 1 AS days_expected
    FROM {TABLE}
""").show()

# 3. Show the latest rows
spark.sql(f"SELECT * FROM {TABLE} ORDER BY weather_date DESC LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM test.dbo.bronze_weather LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
