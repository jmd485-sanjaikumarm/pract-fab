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
# ---------- SOURCE 1 (primary system) ----------
spark.createDataFrame([
    ("101", "Ravi Ltd", "Leeds", "2025-01-10"),
    ("102", "Sita Ltd", "York",  "2025-03-05"),
], "client_id string, client_name string, city string, created_date string") \
 .write.mode("overwrite").saveAsTable("src1_client")

spark.createDataFrame([
    ("5001", "101", "CC", "1000", "2026-09-01"),
    ("5002", "102", "PO", "500",  "2026-09-05"),
    ("5003", "101", "PO", "700",  "2026-09-20"),
], "order_id string, client_id string, product string, amount string, order_date string") \
 .write.mode("overwrite").saveAsTable("src1_orders")

# ---------- SOURCE 2 (acquired company: different names) ----------
spark.createDataFrame([
    ("1", "Kumar Ltd", "London",  "2024-11-01"),
    ("2", "Priya Ltd", "Croydon", "2025-06-15"),
], "cust_no string, cust_nm string, town string, open_dt string") \
 .write.mode("overwrite").saveAsTable("src2_customer")

spark.createDataFrame([
    ("9001", "1", "COMHH",   "2000", "2026-09-02"),
    ("9002", "2", "PROPOWN", "300",  "2026-09-10"),
    ("9003", "1", "ZZ",      "150",  "2026-09-15"),   # unknown code → exception
], "sale_ref string, cust_no string, prod_cd string, value string, sale_dt string") \
 .write.mode("overwrite").saveAsTable("src2_sales")

print("Raw tables created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Which raw table holds each entity for each source
spark.createDataFrame([
("SRC1", "client", "src1_client", "Y"),
("SRC1", "order", "src1_orders", "Y"),
("SRC2", "client", "src2_customer", "Y"),
("SRC2", "order", "src2_sales", "Y"),
], "source string, entity string, raw_table string, active string") \
.write.mode("overwrite").saveAsTable("config_source_entity")
# 2. Column mapping: source column (or expression) → standard column
spark.createDataFrame([
# SRC1 client
("SRC1", "client", "client_id", "client_id"),
("SRC1", "client", "client_name", "client_name"),
("SRC1", "client", "city", "city"),
("SRC1", "client", "client_since", "created_date"),
# SRC1 order
("SRC1", "order", "order_id", "order_id"),
("SRC1", "order", "client_id", "client_id"),
("SRC1", "order", "product_code", "product"),
("SRC1", "order", "amount", "amount"),
("SRC1", "order", "order_date", "order_date"),
# SRC2 client
("SRC2", "client", "client_id", "cust_no"),
("SRC2", "client", "client_name", "cust_nm"),
("SRC2", "client", "city", "town"),
("SRC2", "client", "client_since", "open_dt"),
# SRC2 order
("SRC2", "order", "order_id", "sale_ref"),
("SRC2", "order", "client_id", "cust_no"),
("SRC2", "order", "product_code", "prod_cd"),
("SRC2", "order", "amount", "value"),
("SRC2", "order", "order_date", "sale_dt"),
], "source string, entity string, std_column string, source_expr string") \
.write.mode("overwrite").saveAsTable("config_column_mapping")

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

df = spark.sql("SELECT * FROM test.dbo.config_column_mapping LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Product code mapping: source code → group code
spark.createDataFrame([
("SRC1", "CC", "COMM_COMBINED"),
("SRC1", "PO", "PROPERTY_OWNERS"),
("SRC2", "COMHH", "COMM_COMBINED"),
("SRC2", "PROPOWN", "PROPERTY_OWNERS"),
], "source string, source_code string, group_product string") \
.write.mode("overwrite").saveAsTable("map_product")
# 4. Group product list (for dim_product)
spark.createDataFrame([
("COMM_COMBINED", "Commercial Combined", "Commercial"),
("PROPERTY_OWNERS", "Property Owners", "Commercial"),
("CONTRACTORS", "Contractors", "Commercial"),
("UNMAPPED", "Unmapped", "Unknown"),
], "group_product string, product_name string, product_line string") \
.write.mode("overwrite").saveAsTable("ref_product")
print("Config tables created")

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

df = spark.sql("SELECT * FROM test.dbo.ref_product LIMIT 1000")
display(df)

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

df = spark.sql("SELECT * FROM test.dbo.ref_product LIMIT 1000")
display(df)

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

df = spark.sql("SELECT * FROM test.dbo.src1_orders LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM test.dbo.src2_sales LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
