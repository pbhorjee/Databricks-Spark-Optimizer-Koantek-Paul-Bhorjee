-- Databricks coffee source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/coffeeshop_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Parsing JSON Data

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

SELECT customer_id, profile:first_name, profile:address:country 
FROM customers

-- COMMAND ----------

SELECT from_json(profile) AS profile_struct
  FROM customers;

-- COMMAND ----------

SELECT profile 
FROM customers 
LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) AS profile_struct
  FROM customers;
  
SELECT * FROM parsed_customers

-- COMMAND ----------

DESCRIBE parsed_customers

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;
  
SELECT * FROM customers_final

-- COMMAND ----------

SELECT order_id, customer_id, coffee
FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explode Function

-- COMMAND ----------

SELECT order_id, customer_id, explode(coffee) AS coffee 
FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Collecting Rows

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(coffee.coffee_id) AS coffee_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Flatten Arrays

-- COMMAND ----------

SELECT customer_id,
  collect_set(coffee.coffee_id) As before_flatten,
  array_distinct(flatten(collect_set(coffee.coffee_id))) AS after_flatten
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Join Operations

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(coffee) AS coffee 
  FROM orders) o
INNER JOIN coffee b
ON o.coffee.coffee_id = b.coffee_id;

SELECT * FROM orders_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Operations

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.coffeeshop}/orders-new`;

SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates 

-- COMMAND ----------

SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates 

-- COMMAND ----------

SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reshaping Data with Pivot

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    coffee.coffee_id AS coffee_id,
    coffee.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR coffee_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions
