-- Databricks coffee source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/coffeeshop_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`${dataset.coffeeshop}/orders`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwriting Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.coffeeshop}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.coffeeshop}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`${dataset.coffeeshop}/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending Data

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.coffeeshop}/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merging Data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.coffeeshop}/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW coffee_updates
   (coffee_id STRING, title STRING, coffee STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.coffeeshop}/coffee-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM coffee_updates

-- COMMAND ----------

MERGE INTO coffee b
USING coffee_updates u
ON b.coffee_id = u.coffee_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
  INSERT *
