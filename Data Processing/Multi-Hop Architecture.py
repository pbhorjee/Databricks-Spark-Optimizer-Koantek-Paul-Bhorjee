# Databricks coffee source
# MAGIC %md-sandbox


# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source dDirectory

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_coffeeshop}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Auto Loader

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/files/checkpoints/orders_raw")
    .load(f"{dataset_coffeeshop}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Enriching Raw Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
# MAGIC   SELECT *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC   FROM orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Bronze Table

# COMMAND ----------

(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/files/checkpoints/orders_bronze")
      .outputMode("append")
      .table("orders_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating Static Lookup Table

# COMMAND ----------

(spark.read
      .format("json")
      .load(f"{dataset_coffeeshop}/customers-json")
      .createOrReplaceTempView("customers_lookup"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Silver Table

# COMMAND ----------

(spark.readStream
  .table("orders_bronze")
  .createOrReplaceTempView("orders_bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC          cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, coffee
# MAGIC   FROM orders_bronze_tmp o
# MAGIC   INNER JOIN customers_lookup c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0)

# COMMAND ----------

(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/files/checkpoints/orders_silver")
      .outputMode("append")
      .table("orders_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Gold Table

# COMMAND ----------

(spark.readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_coffee_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) coffee_counts
# MAGIC   FROM orders_silver_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC   )

# COMMAND ----------

(spark.table("daily_customer_coffee_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/files/checkpoints/daily_customer_coffee")
      .trigger(availableNow=True)
      .table("daily_customer_coffee"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_coffee

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Stopping active streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
