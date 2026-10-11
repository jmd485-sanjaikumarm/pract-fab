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
