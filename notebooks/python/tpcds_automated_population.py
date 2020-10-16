# Databricks notebook source
dbutils.widgets.text("delta_path", "/tpcds_populate_automate/delta")
dbutils.widgets.text("raw_data_path", "/tpcds_populate_automate/data")
dbutils.widgets.text("scale_factor", "2")
dbutils.widgets.text("db", "tpcds_migrate_sf1")

# COMMAND ----------

import io
import json 
import os
import subprocess

from pyspark.sql.types import (
  StringType,
  IntegerType,
  DataType,
  DateType,
  DecimalType,
  StructType,
  StructField
)

# COMMAND ----------

os.chdir("/databricks/driver/")
apt_get_out = subprocess.run(["sudo", "apt-get", "-y", "install", "gcc", "make", "flex", "bison", "byacc", "git"], 
  stdout=subprocess.PIPE, 
  text=True,)
git_clone_out = subprocess.run(["git", "clone", "https://github.com/fartzy/tpcds-kit.git",],
  stdout=subprocess.PIPE, 
  text=True,)
os.chdir("/databricks/driver/tpcds-kit/tools")
ls_out = subprocess.run(["make", "OS=LINUX",],
  stdout=subprocess.PIPE, 
  text=True,)

# COMMAND ----------

#get parameter values
delta_path = dbutils.widgets.get("delta_path")
raw_data_path = dbutils.widgets.get("raw_data_path")
schemas_path = "{raw_data_path}/schemas".format(raw_data_path=raw_data_path)
sf = dbutils.widgets.get("scale_factor")
db = dbutils.widgets.get("db")

#initialize directories and database creation
dbutils.fs.mkdirs(delta_path)
dbutils.fs.mkdirs(raw_data_path)
dbutils.fs.mkdirs(schemas_path)
sql("""
  CREATE DATABASE {db} IF NOT EXISTS 
  COMMENT 'This was created for the TPC-DS Auto Generation' 
  LOCATION '{delta_path}'
  """.format(db=db, delta_path=delta_path))

# COMMAND ----------

os.chdir("/databricks/driver/tpcds-kit/tools")
dsdgen_out = subprocess.run(
  [
    "./dsdgen", 
    "-SCALE", 
    "{sf}".format(sf=sf),
    "-DIR", 
    "/dbfs{raw_data_path}".format(raw_data_path=raw_data_path),
    "-FORCE", 
    "Y",
    "-SCHEMAS", 
    "Y", 
    "-VERBOSE", 
    "Y"
  ],
  stdout=subprocess.PIPE, 
  text=True,)

# COMMAND ----------

def subprocess_cmd(command):
    process = subprocess.Popen(command,stdout=subprocess.PIPE, shell=True)
    proc_stdout = process.communicate()[0].strip()
    print(proc_stdout)
    return proc_stdout

#subprocess_cmd('cd /databricks/driver/tpcds-kit/tools; make OS=LINUX')
# subprocess_cmd("cd /databricks/driver/tpcds-kit/tools; ./dsdgen -SCALE {sf} -DIR /dbfs{raw_data_path} -FORCE Y -SCHEMAS Y -VERBOSE Y".format(
#   sf=sf, raw_data_path=raw_data_path
# ))

# COMMAND ----------

sum = 0
for file in dbutils.fs.ls(raw_data_path):
  sum = sum + file.size
  
print(str(round((sum / 1024 / 1024 / 1024), 3)) + " GB Total Size of Raw Data Files" )

# COMMAND ----------

# get all schemas
schemas = [schema[1].replace('.json', '') for schema in dbutils.fs.ls(schemas_path)]

# This is for DEBUGGING only 
# to add a single schema at a time
# schemas = ['web_site']

for table_name in schemas:

  print("Table Name: " + table_name, end="\n\n")

  with open("/dbfs/{schemas_path}/{table_name}.json".format(table_name=table_name, schemas_path=schemas_path), 'r+') as f:
    current_schema = json.load(f)

    col_list = []
    for col_name, type_str in current_schema['columns'].items():
      col_list.append(
        StructField(
          col_name, 
          IntegerType() if type_str == "INT" else ( 
            StringType() if type_str == "STRING" else ( DateType() if type_str == "DATE" else DecimalType(7,2) ) 
          )
        )
      )

    df_schema = StructType(col_list)
    print(df_schema)
    
  df = (
    spark
      .read
      .schema(df_schema)
      .option("delimiter", "|")
      .csv("{raw_data_path}/{table_name}.dat".format(
        table_name=table_name,
        raw_data_path=raw_data_path 
      ))
  )
    
  (
    df
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save("{delta_path}/{table_name}".format(
        table_name=table_name, 
        delta_path=delta_path
      ))
  )
  
  spark.sql("""
    CREATE TABLE IF NOT EXISTS {db}.{table_name}
    USING DELTA
    LOCATION '{delta_path}/{table_name}'
  """.format(
    db=db, 
    delta_path=delta_path, 
    table_name=table_name))
  
  display(
    spark.sql("""
    SELECT * 
    FROM {db}.{table_name}
  """.format(db=db, table_name=table_name)
    )
  )
