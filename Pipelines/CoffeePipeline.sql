-- Databricks coffee source
SET datasets.path=dbfs:/mnt/files-datasets/coffeeshop;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE coffee_bronze
COMMENT "The raw coffee data, ingested from CDC feed"
AS SELECT * FROM cloud_files("${datasets.path}/coffee-cdc", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE coffee_silver;

APPLY CHANGES INTO LIVE.coffee_silver
  FROM STREAM(LIVE.coffee_bronze)
  KEYS (coffee_id)
  APPLY AS DELETE WHEN row_status = "DELETE"
  SEQUENCE BY row_time
  COLUMNS * EXCEPT (row_status, row_time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Layer Tables

-- COMMAND ----------

CREATE LIVE TABLE coffee_counts_state
  COMMENT "Number of coffee per coffee"
AS SELECT coffee, count(*) as coffee_count, current_timestamp() updated_time
  FROM LIVE.coffee_silver
  GROUP BY coffee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DLT Views

-- COMMAND ----------

CREATE LIVE VIEW coffee_sales
  AS SELECT b.title, o.quantity
    FROM (
      SELECT *, explode(coffee) AS coffee 
      FROM LIVE.orders_cleaned) o
    INNER JOIN LIVE.coffee_silver b
    ON o.coffee.coffee_id = b.coffee_id;
