# Databricks notebook source

container = "newcontainer"
storageAccount = "adbnew"
accessKey = dbutils.secrets.get(scope = "KV77", key = "KEY-ACCESS-NEW")
 
accountKey = "fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount)
#SAS = "fs.azure.sas.{}.{}.blob.core.windows.net".format(container，storageAccount)
# Set the credentials to Spark configuration
spark.conf.set(
  accountKey,
  accessKey)

# Set the access key also in SparkContext to be able to access blob in RDD
# Hadoop configuration options set using spark.conf.set(...) are not accessible via SparkContext..
# This means that while they are visible to the DataFrame and Dataset API, they are not visible to the RDD API.

spark._jsc.hadoopConfiguration().set(
  accountKey,
  accessKey)

# Mount the drive for native python
inputSource = "wasbs://{}@{}.blob.core.windows.net".format(container, storageAccount)
mountPoint = "/mnt/" + container
extraConfig = {accountKey: accessKey}

print("Mounting: {}".format(mountPoint))



try:
  dbutils.fs.mount(
    source = inputSource,
    mount_point = str(mountPoint),
    extra_configs = extraConfig
  )
  print("=> Succeeded")
except Exception as e:
  if "Directory already mounted" in str(e):
    print("=> Directory {} already mounted".format(mountPoint))
  else:
    raise(e)

# COMMAND ----------


