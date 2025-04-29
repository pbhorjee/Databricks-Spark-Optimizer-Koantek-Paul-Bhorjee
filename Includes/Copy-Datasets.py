# Databricks coffee source
def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

def download_dataset(source, target):
    files = dbutils.fs.ls(source)

    for f in files:
        source_path = f"{source}/{f.name}"
        target_path = f"{target}/{f.name}"
        if not path_exists(target_path):
            print(f"Copying {f.name} ...")
            dbutils.fs.cp(source_path, target_path, True)

# COMMAND ----------

data_source_uri = "s3://pbhorjee-koantek/datasets/coffeeshop/"
dataset_coffeeshop = 'dbfs:/mnt/files-datasets/coffeeshop'
data_catalog = 'hive_metastore'
spark.conf.set(f"dataset.coffeeshop", dataset_coffeeshop)
spark.conf.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

# COMMAND ----------

def get_index(dir):
    files = dbutils.fs.ls(dir)
    index = 0
    if files:
        file = max(files).name
        index = int(file.rsplit('.', maxsplit=1)[0])
    return index+1

# COMMAND ----------

def set_current_catalog(catalog_name):
    spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# Structured Streaming
streaming_dir = f"{dataset_coffeeshop}/orders-streaming"
raw_dir = f"{dataset_coffeeshop}/orders-raw"

def load_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.parquet"
    print(f"Loading {latest_file} file to the coffeeshop dataset")
    dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")

    
def load_new_data(all=False):
    index = get_index(raw_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_file(index)
            index += 1
    else:
        load_file(index)
        index += 1

# COMMAND ----------

# DLT
streaming_orders_dir = f"{dataset_coffeeshop}/orders-json-streaming"
streaming_coffee_dir = f"{dataset_coffeeshop}/coffee-streaming"

raw_orders_dir = f"{dataset_coffeeshop}/orders-json-raw"
raw_coffee_dir = f"{dataset_coffeeshop}/coffee-cdc"

def load_json_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.json"
    print(f"Loading {latest_file} orders file to the coffeeshop dataset")
    dbutils.fs.cp(f"{streaming_orders_dir}/{latest_file}", f"{raw_orders_dir}/{latest_file}")
    print(f"Loading {latest_file} coffee file to the coffeeshop dataset")
    dbutils.fs.cp(f"{streaming_coffee_dir}/{latest_file}", f"{raw_coffee_dir}/{latest_file}")

    
def load_new_json_data(all=False):
    index = get_index(raw_orders_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_json_file(index)
            index += 1
    else:
        load_json_file(index)
        index += 1

# COMMAND ----------

download_dataset(data_source_uri, dataset_coffeeshop)
set_current_catalog(data_catalog)
