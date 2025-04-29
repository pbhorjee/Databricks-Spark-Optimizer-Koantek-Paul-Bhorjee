# Databricks coffee source
files = dbutils.fs.ls("dbfs:/mnt/files/dlt/demo_coffeeshop")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/files/dlt/demo_coffeeshop/system/events")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/mnt/files/dlt/demo_coffeeshop/system/events`

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/files/dlt/demo_coffeeshop/tables")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.demo_coffeeshop_dlt_db.cn_daily_customer_coffee

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.demo_coffeeshop_dlt_db.fr_daily_customer_coffee
