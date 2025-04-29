-- Databricks coffee source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/coffeeshop_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_coffeeshop}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM json.`${dataset.coffeeshop}/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset.coffeeshop}/customers-json/export_*.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset.coffeeshop}/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset.coffeeshop}/customers-json`

-- COMMAND ----------

 SELECT *,
    input_file_name() source_file
  FROM json.`${dataset.coffeeshop}/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format

-- COMMAND ----------

SELECT * FROM text.`${dataset.coffeeshop}/customers-json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.coffeeshop}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 

-- COMMAND ----------

SELECT * FROM csv.`${dataset.coffeeshop}/coffee-csv`

-- COMMAND ----------

CREATE TABLE coffee_csv
  (coffee_id STRING, title STRING, coffee STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.coffeeshop}/coffee-csv"

-- COMMAND ----------

SELECT * FROM coffee_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables

-- COMMAND ----------

DESCRIBE EXTENDED coffee_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_coffeeshop}/coffee-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC         .table("coffee_csv")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{dataset_coffeeshop}/coffee-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_coffeeshop}/coffee-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT COUNT(*) FROM coffee_csv

-- COMMAND ----------

REFRESH TABLE coffee_csv

-- COMMAND ----------

SELECT COUNT(*) FROM coffee_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`${dataset.coffeeshop}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE coffee_unparsed AS
SELECT * FROM csv.`${dataset.coffeeshop}/coffee-csv`;

SELECT * FROM coffee_unparsed;

-- COMMAND ----------

CREATE TEMP VIEW coffee_tmp_vw
   (coffee_id STRING, title STRING, coffee STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.coffeeshop}/coffee-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE coffee AS
  SELECT * FROM coffee_tmp_vw;
  
SELECT * FROM coffee

-- COMMAND ----------

DESCRIBE EXTENDED coffee
