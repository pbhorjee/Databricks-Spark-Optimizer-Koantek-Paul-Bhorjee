# Databricks coffee source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

load_new_json_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from json.`${dataset.coffeeshop}/coffee-cdc/02.json`
