# Databricks notebook source
dbutils.widgets.text("path_prefix", "/tpcds_populate_automate")
dbutils.widgets.text("scale_factor", "100")
dbutils.widgets.text("db_prefix", "tpcds_migrate_sf")

# COMMAND ----------

import fnmatch
import glob
import io
import json 
import multiprocessing
import os
import re
import shutil
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

# download code for tpcds-kit and then install

# install dependant packages
os.chdir("/databricks/driver/")
get_packages_out = subprocess.run(["sudo", "apt-get", "-y", "install", "gcc", "make", "flex", "bison", "byacc", "git"], 
  stdout=subprocess.PIPE, 
  stderr=subprocess.STDOUT,
  text=True,)
print("\noutput of package download:\n{get_packages_out}".format(get_packages_out=get_packages_out))

# download code from git repo
download_code_out = subprocess.run(["git", "clone", "https://github.com/fartzy/tpcds-kit.git",],
  stdout=subprocess.PIPE, 
  stderr=subprocess.STDOUT, 
  text=True,)
print("\noutput of download code:\n{download_code_out}".format(download_code_out=download_code_out))

# cd to tools dir and execute make
os.chdir("/databricks/driver/tpcds-kit/tools")
install_out = subprocess.run(["make", "OS=LINUX",],
  stdout=subprocess.PIPE, 
  stderr=subprocess.STDOUT,
  text=True,)
print("\noutput of make:\n{install_out}".format(install_out=install_out))

# COMMAND ----------

#get parameter values
sf = dbutils.widgets.get("scale_factor")
db = dbutils.widgets.get("db_prefix") + "_{sf}".format(sf=sf)

raw_data_path = dbutils.widgets.get("path_prefix") + "_{sf}/data".format(sf=sf)
schemas_path = dbutils.widgets.get("path_prefix") + "_{sf}/data/schemas".format(sf=sf)
dbfs_script_path = dbutils.widgets.get("path_prefix") + "_{sf}/script".format(sf=sf)
delta_path = dbutils.widgets.get("path_prefix") + "_{sf}/delta".format(sf=sf)

# print out variables
print("scale factor: {sf}".format(sf=sf))
print("database: {db}".format(db=db))
print("raw data path: {raw_data_path}".format(raw_data_path=raw_data_path))
print("schemas path: {schemas_path}".format(schemas_path=schemas_path))
print("dbfs script path: {dbfs_script_path}".format(dbfs_script_path=dbfs_script_path))
print("delta tables path: {delta_path}".format(delta_path=delta_path))

# create directories and database if needed
dbutils.fs.mkdirs(raw_data_path)
dbutils.fs.mkdirs(schemas_path)
dbutils.fs.mkdirs(dbfs_script_path)
dbutils.fs.mkdirs(delta_path)

sql("""
  CREATE DATABASE IF NOT EXISTS {db}
  COMMENT 'This was created for TPC-DS data auto generation with scale factor of {sf}' 
  LOCATION '{delta_path}'
  """.format(db=db, delta_path=delta_path, sf=sf))

# COMMAND ----------

# create fuse script 
script_path = "/dbfs{dbfs_script_path}/dsdgen.sh".format(dbfs_script_path=dbfs_script_path)
os.chdir("/databricks/driver/tpcds-kit/tools")

# use all available processes except for one for overhead 
N = multiprocessing.cpu_count() - 1

# create a script to execute - one long string would have to use the shell arg set to True
with open(script_path,"w") as f:

  # small n is child argument - up to N parallel processes
  for n in range(1, N + 1):
    
    # add shell shebang to top of script
    if n == 1:
      f.write("#!/bin/sh\n")
      
    # execute in background and run in parallel  
    f.write("./dsdgen -scale {sf} -f -dir /dbfs{raw_data_path} -FORCE Y -PARALLEL {N} -SCHEMAS Y -VERBOSE Y -CHILD {n} & ".format(
      raw_data_path=raw_data_path,
      sf=sf,
      n=n,
      N=N
      )
    )
    
dsdgen_out = subprocess.run([script_path, ],stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,)

print("output of dsdgen script:\n{dsdgen_out}".format(dsdgen_out=dsdgen_out))

# COMMAND ----------

# print total data size to screen

sum = 0
for file in dbutils.fs.ls(raw_data_path):
  sum = sum + file.size
  
print(str(round((sum / 1024 / 1024 / 1024), 3)) + " GB Total Size of Raw Data Files" )

# COMMAND ----------

tables = set()
fuse_raw_data_path = "/dbfs{raw_data_path}/".format(raw_data_path=raw_data_path)

#create set of table names from data files
for file in os.listdir(fuse_raw_data_path):
  if fnmatch.fnmatch(file, '*_[1-9]*_[1-9]*.dat'):
    tables.add(re.search(r'^(\w+)_\d+_\d+', file).group(1))

for table in tables:
  table_directory = "{fuse_raw_data_path}/{table}".format(fuse_raw_data_path=fuse_raw_data_path, table=table)

  if not os.path.exists(table_directory):
    print("making " + table_directory)
    os.makedirs(table_directory)
    
  #print("glob for {table}:".format(table=table))
  
  for file_path in glob.glob("{fuse_raw_data_path}/{table}_[1-9]*_[1-9]*.dat".format(fuse_raw_data_path=fuse_raw_data_path, table=table)):
    print("moving " + file_path + " to " + "{fuse_raw_data_path}/{table}/{file}".format(fuse_raw_data_path=fuse_raw_data_path, table=table, file=os.path.basename(file_path)))
    shutil.move(file_path, "{fuse_raw_data_path}/{table}/{file}".format(fuse_raw_data_path=fuse_raw_data_path, table=table, file=os.path.basename(file_path)))
    

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
            StringType() if type_str == "STRING" else ( 
              DateType() if type_str == "DATE" else DecimalType(7,2) ) 
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
      .csv("{raw_data_path}/{table_name}_[1-9]*_{total_child}.*".format(
        table_name=table_name,
        raw_data_path=raw_data_path,
        total_child=str(N)
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
