# Databricks notebook source
# DBTITLE 1,Silver Layer Overview
# MAGIC %md
# MAGIC ### 03 — Star Schema Definition via PySpark DataFrame API
# MAGIC
# MAGIC #### What are we doing?
# MAGIC We are defining a **star schema** using the PySpark `StructType` / `StructField` API. The star schema breaks the flat bronze table into **1 fact table** and **5 dimension tables**, optimised for analytical queries.
# MAGIC
# MAGIC | Table | Type | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | `fact_violation` | Fact | Central table — one row per violation event, linking to all dimensions |
# MAGIC | `dim_business` | Dimension | Restaurant identity, facility type, and risk level |
# MAGIC | `dim_location` | Dimension | Address, city, state, zip, lat/lon |
# MAGIC | `dim_date` | Dimension | Date breakdown (year, month, day, quarter, day-of-week) |
# MAGIC | `dim_inspection_type` | Dimension | Type of inspection performed |
# MAGIC | `dim_violation_type` | Dimension | Violation code and description |
# MAGIC
# MAGIC #### How does this connect to what has been done so far?
# MAGIC
# MAGIC | Layer | Step | Notebook | What it did |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **Bronze** | 1 | `00-Input-Data` | Read CSV → wrote raw Delta table (307,282 rows) |
# MAGIC | **Bronze** | 2 | `01-Data-Profiling` | Profiled data quality, cleaned City/Risk/Facility_Type, dropped invalid rows (307,018 rows) |
# MAGIC | **Bronze** | 3 | `02-Schema-Enforcement` | Applied `NOT NULL` constraints on 12 required columns |
# MAGIC | **Gold** | **4** | **This notebook** | **Defines the star schema that the bronze data will be transformed into** |
# MAGIC
# MAGIC > **Note:** A silver layer (dedup, type casting, enrichment) could sit between bronze and gold, but this notebook focuses on defining the target gold schema. The flat bronze table gets decomposed into normalised dimensions linked by surrogate keys in the fact table.

# COMMAND ----------

# DBTITLE 1,Define target silver schema with StructType
# PySpark types used to define each table's schema in the star schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

# COMMAND ----------

# DBTITLE 1,dim_business schema
# Dimension: Business — one row per unique restaurant/business entity
# Sourced from: License, DBA_Name, AKA_Name, Facility_Type, Risk in the bronze table
dim_business_schema = StructType([
    StructField("business_key",    IntegerType(), nullable=False),
    StructField("license_number",  IntegerType(), nullable=False),
    StructField("dba_name",        StringType(),  nullable=False),
    StructField("aka_name",        StringType(),  nullable=True),
    StructField("facility_type",   StringType(),  nullable=False),
    StructField("risk",            StringType(),  nullable=False),
])

print("dim_business:", dim_business_schema.simpleString())

# COMMAND ----------

# DBTITLE 1,dim_location schema
# Dimension: Location — one row per unique physical address
# Sourced from: Address, City, State, Zip, Latitude, Longitude in the bronze table
dim_location_schema = StructType([
    StructField("location_key",  IntegerType(), nullable=False),
    StructField("address",       StringType(),  nullable=False),
    StructField("city",          StringType(),  nullable=False),
    StructField("state",         StringType(),  nullable=False),
    StructField("zip",           IntegerType(), nullable=False),
    StructField("latitude",      DoubleType(),  nullable=False),
    StructField("longitude",     DoubleType(),  nullable=False),
])

print("dim_location:", dim_location_schema.simpleString())

# COMMAND ----------

# DBTITLE 1,dim_date schema
# Dimension: Date — one row per unique calendar date for time-based slicing
# Sourced from: Inspection_Date in the bronze table (decomposed into year, month, day, etc.)
dim_date_schema = StructType([
    StructField("date_key",    IntegerType(), nullable=False),
    StructField("full_date",   DateType(),    nullable=False),
    StructField("year",        IntegerType(), nullable=False),
    StructField("month",       IntegerType(), nullable=False),
    StructField("day",         IntegerType(), nullable=False),
    StructField("day_of_week", StringType(),  nullable=False),
    StructField("quarter",     IntegerType(), nullable=False),
])

print("dim_date:", dim_date_schema.simpleString())

# COMMAND ----------

# DBTITLE 1,dim_inspection_type schema
# Dimension: Inspection Type — one row per unique type of inspection
# Sourced from: Inspection_Type in the bronze table
dim_inspection_type_schema = StructType([
    StructField("inspection_type_key", IntegerType(), nullable=False),
    StructField("inspection_type",     StringType(),  nullable=False),
])

print("dim_inspection_type:", dim_inspection_type_schema.simpleString())

# COMMAND ----------

# DBTITLE 1,dim_violation schema
# Dimension: Violation Type — one row per unique violation code + description
# Sourced from: Violations in the bronze table (parsed into code and description)
dim_violation_type_schema = StructType([
    StructField("violation_type_key",    IntegerType(), nullable=False),
    StructField("violation_code",        IntegerType(), nullable=False),
    StructField("violation_description", StringType(),  nullable=True),
])

print("dim_violation_type:", dim_violation_type_schema.simpleString())

# COMMAND ----------

# DBTITLE 1,fact_inspection schema
# Fact: Violation — central table, one row per violation event
# Links to dimensions via foreign keys; result and violation_comment stored directly
fact_violation_schema = StructType([
    StructField("violation_id",         IntegerType(), nullable=False),
    StructField("inspection_id",        IntegerType(), nullable=False),
    StructField("business_key",         IntegerType(), nullable=False),
    StructField("location_key",         IntegerType(), nullable=False),
    StructField("date_key",             IntegerType(), nullable=False),
    StructField("inspection_type_key",  IntegerType(), nullable=False),
    StructField("result",               StringType(),  nullable=False),
    StructField("violation_type_key",   IntegerType(), nullable=False),
    StructField("violation_comment",    StringType(),  nullable=True),
])

print("fact_violation:", fact_violation_schema.simpleString())

# COMMAND ----------

# DBTITLE 1,Star schema summary
# All schemas collected for easy reference and iteration
all_schemas = {
    "fact_violation":       fact_violation_schema,
    "dim_business":         dim_business_schema,
    "dim_location":         dim_location_schema,
    "dim_date":             dim_date_schema,
    "dim_inspection_type":  dim_inspection_type_schema,
    "dim_violation_type":   dim_violation_type_schema,
}

total_cols = sum(len(s.fields) for s in all_schemas.values())
print(f"Star schema: 1 fact table + 5 dimension tables = {total_cols} total columns")
