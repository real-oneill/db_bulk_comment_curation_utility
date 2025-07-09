# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## BEFORE RUNNING THIS NOTEBOOK
# MAGIC 1. Optionally run notebook `01 Export Comments`
# MAGIC 2. Download the Csv file from the `staging/output` folder of the volume
# MAGIC 3. Edit the comments offline in the Csv file
# MAGIC 4. Upload the curated Csv file to the `staging/input` folder of the Unity Catalog Volume.
# MAGIC
# MAGIC The default file is named `curated_metadata.csv` but you can specify this in the widgets.
# MAGIC
# MAGIC The file should contain the following columns:
# MAGIC
# MAGIC <table>
# MAGIC    <thead>
# MAGIC       <tr>
# MAGIC          <th>Column Name</th>
# MAGIC          <th>Description</th>
# MAGIC       </tr>
# MAGIC    </thead>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td>type</td>
# MAGIC          <td>Type of asset, can be table or column.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>table_catalog</td>
# MAGIC          <td>The catalog containing the tables and columns to be updated.</td>
# MAGIC       </tr>
# MAGIC        <tr>
# MAGIC          <td>table_schema</td>
# MAGIC          <td>The schema containing the tables and columns to be updated.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>table_name</td>
# MAGIC          <td>Name of table containing comments and columns.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>column_name</td>
# MAGIC          <td>Columns containing comments. Will be null for table records.</td>
# MAGIC       </tr>
# MAGIC      <tr>
# MAGIC          <td>comment</td>
# MAGIC          <td>The table or column comment that has been manually adjusted by an expert reviewer.</td>
# MAGIC       </tr>
# MAGIC    </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Widgets
# MAGIC
# MAGIC **staging_catalog**: The catalog that contains the bulk_comments_metadata schema and staging volume. This is where you have uploaded the curated Csv file.

# COMMAND ----------

dbutils.widgets.text("staging_catalog", "", "Enter Staging Catalog:")
dbutils.widgets.text("input_file_name", "curated_metadata.csv", "Enter Input File Name:")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ${staging_catalog}.bulk_comment_curation;
# MAGIC CREATE VOLUME IF NOT EXISTS ${staging_catalog}.bulk_comment_curation.staging;
# MAGIC
# MAGIC USE CATALOG ${staging_catalog};
# MAGIC USE SCHEMA bulk_comment_curation;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Csv file from Volume

# COMMAND ----------


# Set variables

staging_catalog = dbutils.widgets.get("staging_catalog")
input_file_name = dbutils.widgets.get("input_file_name")
staging_path = f"/Volumes/{staging_catalog}/bulk_comment_curation/staging/input/{input_file_name}"
print(staging_path)

# COMMAND ----------

# Read file and check formatting

from pyspark.sql.utils import AnalysisException

# Check for proper file type
if not staging_path.endswith('.csv'):
    raise ValueError("File is not a CSV.")

try:
    # Read the file
    df = spark.read.option("header", "true").csv(staging_path)
    
    # Check for required columns
    required_columns = ["type", "table_catalog", "table_schema", "table_name", "column_name", "comment"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
    print(f"{input_file_name} imported successfully.")
except AnalysisException as e:
    raise ValueError(f"Error reading file: {e}")

df.createOrReplaceTempView("updated_metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create list of new/changed comments
# MAGIC Use the working table for performance here instead of reading from system tables each time.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE changes AS
# MAGIC
# MAGIC WITH current_metadata AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM metadata
# MAGIC   WHERE table_schema != 'information_schema'
# MAGIC     AND table_catalog IN (SELECT DISTINCT table_catalog FROM updated_metadata)
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   b.*,
# MAGIC   a.comment as old_comment
# MAGIC FROM current_metadata a
# MAGIC INNER JOIN updated_metadata b
# MAGIC   ON a.table_catalog = b.table_catalog
# MAGIC   AND a.table_schema = b.table_schema
# MAGIC   AND a.table_name = b.table_name
# MAGIC   AND IFNULL(a.column_name, '99999') = IFNULL(b.column_name, '99999')
# MAGIC WHERE a.comment != b.comment
# MAGIC ;
# MAGIC
# MAGIC SELECT * FROM changes;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loop through changes and add/update comments

# COMMAND ----------

# Retrieve the metadata DataFrame from the temporary view
metadata_df = spark.read.table(f"`{staging_catalog}`.bulk_comment_curation.changes")

# COMMAND ----------

# Loop through rows in the DataFrame
for row in metadata_df.collect():
    type, catalog, schema, table, column, comment, old_comment = row
    # Form the SQL command based on type (table or column) and execute it
    comment = comment.replace('"','\\"')
    if type == "table":
        sql_stmt = f'''COMMENT ON TABLE {catalog}.{schema}.{table} IS "{comment}"'''
        print(sql_stmt)
        spark.sql(sql_stmt)
    elif type == "column":
        sql_stmt = f'''ALTER TABLE {catalog}.{schema}.{table} ALTER COLUMN {column} COMMENT "{comment}"'''
        print(sql_stmt)
        spark.sql(sql_stmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge changes back into working table
# MAGIC This last step is only necessary to keep an updated version of the data in case you want to repeat the process later. Right now the table will be overwritten if you rerun the `01 Export Comments` notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO metadata
# MAGIC USING changes
# MAGIC ON metadata.table_catalog = changes.table_catalog
# MAGIC   AND metadata.table_schema = changes.table_schema
# MAGIC   AND metadata.table_name = changes.table_name
# MAGIC   AND IFNULL(metadata.column_name, '99999') = IFNULL(changes.column_name, '99999')
# MAGIC WHEN MATCHED AND metadata.comment != changes.comment THEN
# MAGIC   UPDATE SET comment = changes.comment;