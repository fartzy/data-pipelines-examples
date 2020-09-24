# Databricks notebook source
# MAGIC %md
# MAGIC # Google Cloud Platform - Google Storage Test

# COMMAND ----------

# MAGIC %md
# MAGIC # Cluster Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Import a JSON Service Key file.
# MAGIC 
# MAGIC * Follow the instructions here to create a JSON key file: https://cloud.google.com/storage/docs/authentication#service_accounts
# MAGIC * Copy the JSON key file into DBFS so that it can be accessed by Databricks clusters.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Configure Cluster
# MAGIC 
# MAGIC Add the following Spark properties
# MAGIC 
# MAGIC * spark.hadoop.fs.gs.impl com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
# MAGIC * spark.hadoop.fs.gs.project.id databricks-mike
# MAGIC * spark.hadoop.google.cloud.auth.service.account.enable true
# MAGIC * spark.hadoop.google.cloud.auth.service.account.json.keyfile /dbfs/mnt/michaeleartz/keys/gcp/databricks-mike-xxxxxxx.json

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Install Google Cloud Storage Hadoop Connector 
# MAGIC 
# MAGIC Install the GCS Hadoop Connector:
# MAGIC + Via Databricks Library Manger and Maven: https://docs.cloud.databricks.com/docs/latest/databricks_guide/index.html#01%20Databricks%20Overview/04%20Libraries.html
# MAGIC 
# MAGIC + The exact actifact is: `com.google.cloud.bigdataoss:gcs-connector:1.5.2-hadoop2`
# MAGIC 
# MAGIC + Attach the library to your cluster(s)

# COMMAND ----------

# MAGIC %md
# MAGIC # Test Listing Files from Google Storage

# COMMAND ----------

# MAGIC %fs ls gs://pub/shakespeare

# COMMAND ----------

# MAGIC %fs ls gs://michaeleartz-data-1/loan-stats/

# COMMAND ----------

# MAGIC %md
# MAGIC # Use DataFrames

# COMMAND ----------

loanStatsDF = spark.read.option("header", True).option("inferSchema", True).csv("gs://michaeleartz-data-1/loan-stats/LoanStats3a.csv.gz")
display(loanStatsDF)

# COMMAND ----------

loanStatsDF.count()

# COMMAND ----------

loanStatsDF.limit(1000).write.mode("overwrite").parquet("gs://michaeleartz-data-1/tmp/loanStats.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Text File

# COMMAND ----------

lines = sc.textFile("gs://pub/shakespeare/rose.txt")
words = lines.flatMap(lambda line: line.split())
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.get("spark.hadoop.fs.gs.impl")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val lines = sc.textFile("gs://pub/shakespeare/rose.txt")
# MAGIC val words = lines.flatMap(_.split(" "))
# MAGIC val wordCounts = words.map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
# MAGIC //wordCounts.saveAsTextFile("/tmp/gcp-gs-wordcount.txt")

# COMMAND ----------

# MAGIC %fs head /tmp/gcp-gs-wordcount.txt/part-00000

# COMMAND ----------


