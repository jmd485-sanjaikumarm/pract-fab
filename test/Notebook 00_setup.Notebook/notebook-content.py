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

# Config table: which cities to load (like source_config for acquisitions)
cities = [
    ("Chennai", 13.08,  80.27, "Y"),
    ("London",  51.51,  -0.13, "Y"),
    ("Leeds",   53.80,  -1.55, "Y"),
]
df = spark.createDataFrame(cities, "city string, latitude double, longitude double, active string")
df.write.format("delta").mode("overwrite").saveAsTable("config_cities")

spark.sql("SELECT * FROM config_cities").show()

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
