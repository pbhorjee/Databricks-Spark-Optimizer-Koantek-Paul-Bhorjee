# Databricks coffee source

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading Stream

# COMMAND ----------

(spark.readStream
      .table("coffee")
      .createOrReplaceTempView("coffee_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Displaying Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM coffee_streaming_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT coffee, count(coffee_id) AS total_coffee
# MAGIC FROM coffee_streaming_tmp_vw
# MAGIC GROUP BY coffee

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Unsupported Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * 
# MAGIC  FROM coffee_streaming_tmp_vw
# MAGIC  ORDER BY coffee

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Persisting Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW coffee_counts_tmp_vw AS (
# MAGIC   SELECT coffee, count(coffee_id) AS total_coffee
# MAGIC   FROM coffee_streaming_tmp_vw
# MAGIC   GROUP BY coffee
# MAGIC )

# COMMAND ----------

(spark.table("coffee_counts_tmp_vw")                               
      .writeStream  
      .trigger(processingTime='4 seconds')
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/files/coffee_counts_checkpoint")
      .table("coffee_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM coffee_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding New Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO coffee
# MAGIC values ("B19", "Kona", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming in Batch Mode 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO coffee
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table("coffee_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/files/coffee_counts_checkpoint")
      .table("coffee_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM coffee_counts
