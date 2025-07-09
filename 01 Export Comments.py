# Databricks notebook source
# MAGIC %md
# MAGIC ## BEFORE RUNNING THIS NOTEBOOK
# MAGIC Makle sure you are running in a Unity Catalog enabled workspace, have access to the stystem tables, and have the proper UC permissions. It will be best if you are able to create a new catalog to store the assets created by these notebooks. Otherwise you would need access to an existing catalog.
# MAGIC
# MAGIC ## Define Widgets
# MAGIC **source_catalog**: The catalog containing the tables and columns for which you want to export the comments.
# MAGIC
# MAGIC **staging_catalog**: The catalog in which the output Csv file will be written, and the Csv file with curated comments will be uploaded. _User running this notebook must have `CREATE SCHEMA` and `CREATE VOLUME` permissions on the specified catalog._

# COMMAND ----------

dbutils.widgets.text("source_catalog", "", "Enter Source Catalog:")
dbutils.widgets.text("staging_catalog", "", "Enter Staging Catalog:")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up staging area for output files

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ${staging_catalog}.bulk_comment_curation;
# MAGIC CREATE VOLUME IF NOT EXISTS ${staging_catalog}.bulk_comment_curation.staging;
# MAGIC
# MAGIC USE CATALOG ${staging_catalog};
# MAGIC USE SCHEMA bulk_comment_curation;

# COMMAND ----------

staging_catalog = dbutils.widgets.get("staging_catalog")
staging_path = f"/Volumes/{staging_catalog}/bulk_comment_curation/staging"
dbutils.fs.mkdirs(f"{staging_path}/output")
dbutils.fs.mkdirs(f"{staging_path}/input")
print(staging_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta table to act as working copy of metadata updates
# MAGIC This does two things:
# MAGIC 1. Improves performance of metadata updates since you are reading changes from a Delta table instead of Csv & System tables
# MAGIC 1. Creates a working copy that can be rolled back to specific versions in case of an error

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE metadata AS
# MAGIC
# MAGIC WITH table_metadata AS (
# MAGIC   SELECT
# MAGIC     table_catalog,
# MAGIC     table_schema,
# MAGIC     table_name,
# MAGIC     comment
# MAGIC   FROM system.information_schema.tables
# MAGIC   WHERE table_schema != 'information_schema'
# MAGIC     AND ('${source_catalog}' = '' OR table_catalog = '${source_catalog}')
# MAGIC )
# MAGIC
# MAGIC , column_metadata AS (
# MAGIC   SELECT
# MAGIC     table_catalog,
# MAGIC     table_schema,
# MAGIC     table_name,
# MAGIC     column_name,
# MAGIC     comment
# MAGIC   FROM system.information_schema.columns
# MAGIC   WHERE table_schema != 'information_schema'
# MAGIC     AND ('${source_catalog}' = '' OR table_catalog = '${source_catalog}')
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   'table' AS type,
# MAGIC   table_catalog,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   NULL AS column_name,
# MAGIC   comment
# MAGIC FROM table_metadata
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'column' AS type,
# MAGIC   table_catalog,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   column_name,
# MAGIC   comment
# MAGIC FROM column_metadata
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export comments to CSV

# COMMAND ----------

# Read the Delta table
df = spark.read.table(f"`{staging_catalog}`.bulk_comment_curation.metadata")

# Write the DataFrame to a single CSV file in the specified UC volume
df.coalesce(1).write.csv(f"/Volumes/{staging_catalog}/bulk_comment_curation/staging/output/spark_out", mode="overwrite", header=True)

# COMMAND ----------

# Keep only the CSV and rename with timestamp

from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
new_file_name = f"metadata_output_{timestamp}.csv"

files = dbutils.fs.ls(f"/Volumes/{staging_catalog}/bulk_comment_curation/staging/output/spark_out/")
csv_file = [file for file in files if file.name.endswith('.csv')]
output_file = csv_file[0].path
output_file_name = output_file.split("/")[-1]
print(output_file)
print(output_file_name)
print(new_file_name)

dbutils.fs.mv(f"{output_file}", f"/Volumes/{staging_catalog}/bulk_comment_curation/staging/output/{output_file_name}".replace(output_file_name, new_file_name))

dbutils.fs.rm(f"/Volumes/{staging_catalog}/bulk_comment_curation/staging/output/spark_out", True)