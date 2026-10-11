# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ae9382cc-56b2-40dd-8329-32adbd3d2060",
# META       "default_lakehouse_name": "salestbl",
# META       "default_lakehouse_workspace_id": "dffb1635-0edb-434c-84ad-9175952a3769",
# META       "known_lakehouses": [
# META         {
# META           "id": "ae9382cc-56b2-40dd-8329-32adbd3d2060"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "3545b3e3-0627-4fb2-a671-1813dacc93c6",
# META       "known_warehouses": [
# META         {
# META           "id": "3545b3e3-0627-4fb2-a671-1813dacc93c6",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import * 

df1 = spark.read.format('csv').option('header','true').load('Files/salefile1')
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = df1.withColumn("year",year(col("OrderDate"))).withColumn("month",month(col("OrderDate")))
df1.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = df1.withColumn("firstname",split(col("CustomerName")," ").getItem(0)).withColumn("lastname",split(col("CustomerName"),"").getItem(1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1.write.format('delta').mode('overwrite').saveAsTable("dbo.Sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
