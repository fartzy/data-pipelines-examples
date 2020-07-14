# Databricks notebook source
"""
   Reads from enterprise_data tables and then writes to a dbfs location that any cluster has permissions to read/write 
   **Run with 'prod_commerical_reader' Cluster
"""

dbs = ['customer_data_1_rwd_prod', 'ext_rwd_prod']
tables_to_exclude = []

for db in dbs:
  for table_name in [r['tableName'] for r in spark.sql( \
      "show tables in {db}".format(db=db)).selectExpr("tableName").collect() if r['tableName'] not in tables_to_exclude]:
  
    ingest_df = spark.sql("SELECT * FROM {db}.{table_name}".format(table_name=table_name, db=db))
    ingest_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/glue_test/temp_db/temp_{table_name}".format(table_name=table_name))

# COMMAND ----------

"""
   Registers tables in glue database 'temp_db' for every enterprise_data table
   Tables are prepended with 'temp_'
   Both enterprise_data databases are combined into 'temp_db'
   **Run with 'testGlueIntegration' Cluster
"""

tables_to_exclude = []
for file_info in [file_info for file_info in dbutils.fs.ls("/glue_test/temp_db/") if file_info[1][:-1] not in tables_to_exclude]:
  table = file_info[1][:-1]
  location = file_info[0][5:]
  print("table path = " + location)
  spark.sql("""
    CREATE TABLE IF NOT EXISTS temp_db.temp_{table}
    USING DELTA LOCATION '{location}'
   """.format(table=table, location=location))
  print("row count " + table + " = " + str(spark.sql("SELECT COUNT(*) AS row_count FROM temp_db.temp_{table}".format(table=table)).selectExpr("row_count").collect()[0]['row_count']))
  print()

# COMMAND ----------

# DBTITLE 1,Move All the Tables from 'temp_db' To the two "rwd" databases
"""
   Reads from glue 'temp_db' tables and then writes to a properly named glue database 
   **Run with 'testGlueIntegration' Cluster
"""

dbs = ['temp_db']
target_db = "customer_data_1_rwd"
#tables_to_exclude = ["temp_temp_customer_data_1_otl_diagnosis_data_11_02_2016"]
tables_to_include = ["temp_temp_customer_data_1_otl_diagnosis_data_11_02_2016"]

for db in dbs:
  for table_name in [ \
        r['tableName'] for r in spark.sql( \
       "SHOW TABLES IN {db}".format(db=db)) \
       .selectExpr("tableName").collect() if r['tableName'] in tables_to_include \
      ]:
    table = table_name.replace("temp_", "")
    location = "dbfs:/mnt/prod-enterprise_data/bronze/delta/{target_db}_prod.db/{table}".format(table=table, target_db=target_db)
    print(location)
    ddl_query = """
       CREATE TABLE IF NOT EXISTS {target_db}.{table}
       USING DELTA LOCATION '{location}'
    """.format(table=table, location=location, target_db=target_db)
    
    print(ddl_query)
    #register table 
    spark.sql("""
      CREATE TABLE IF NOT EXISTS {target_db}.{table}
      USING DELTA LOCATION '{location}'
    """.format(table=table, location=location, target_db=target_db))
    
    #check for row counts
    print("row count " + table + " = " + str(spark.sql( \
      """SELECT COUNT(*) AS row_count 
         FROM {target_db}.{table}
      """.format(table=table, target_db=target_db)).selectExpr("row_count").collect()[0]['row_count']))

# COMMAND ----------

# DBTITLE 1,Register enterprise_data Tables In Data Lake Prod Glue 
"""
    Registers commerical from lists of string literals to the glue Prod Catalog
   **Run with 'Prodenterprise_dataWithGlue' Cluster
"""

dbs = ['customer_data_1_rwd']
tables = ["customer_data_1_otl_diagnosis_data_11_02_2016"]
#tables = ["ext_diagnosis_code_otl","ext_diagnosis_otl", "ext_procedure_otl", "ext_prescriber_otl", "ext_patient_otl","ext_product_otl", "ext_procedure_code_otl", "ext_rx_otl"]

for db in dbs:
  for table in tables:
    location = "dbfs:/mnt/prod-enterprise_data/bronze/delta/{db}_prod.db/{table}".format(table=table, db=db)
    print(location)
    ddl_query = """
       CREATE TABLE IF NOT EXISTS {db}.{table}
       USING DELTA LOCATION '{location}'
    """.format(table=table, location=location, db=db)
    
    
    ########register table
    #print(ddl_query)
    spark.sql(ddl_query)
    
    #check for row counts
    print("row count " + table + " = " + str(spark.sql( \
      """SELECT COUNT(*) AS row_count 
         FROM {db}.{table}
      """.format(table=table, db=db)).selectExpr("row_count").collect()[0]['row_count']))
    
########## results 
   ### ext_rwd
# dbfs:/mnt/prod-enterprise_data/bronze/delta/ext_rwd_prod.db/ext_diagnosis_code_otl
# row count ext_diagnosis_code_otl = 48664
# dbfs:/mnt/prod-enterprise_data/bronze/delta/ext_rwd_prod.db/ext_diagnosis_otl
# row count ext_diagnosis_otl = 111015508
# dbfs:/mnt/prod-enterprise_data/bronze/delta/ext_rwd_prod.db/ext_procedure_otl
# row count ext_procedure_otl = 136475943
# dbfs:/mnt/prod-enterprise_data/bronze/delta/ext_rwd_prod.db/ext_prescriber_otl
# row count ext_prescriber_otl = 1588397
# dbfs:/mnt/prod-enterprise_data/bronze/delta/ext_rwd_prod.db/ext_patient_otl
# row count ext_patient_otl = 1258573
# dbfs:/mnt/prod-enterprise_data/bronze/delta/ext_rwd_prod.db/ext_product_otl
# row count ext_product_otl = 60768
# dbfs:/mnt/prod-enterprise_data/bronze/delta/ext_rwd_prod.db/ext_procedure_code_otl
# row count ext_procedure_code_otl = 14824
# dbfs:/mnt/prod-enterprise_data/bronze/delta/ext_rwd_prod.db/ext_rx_otl
# row count ext_rx_otl = 132996359
   ### customer_data_1_rwd
# dbfs:/mnt/prod-enterprise_data/bronze/delta/customer_data_1_rwd_prod.db/customer_data_1_otl_diagnosis_data_11_02_2016
# row count customer_data_1_otl_diagnosis_data_11_02_2016 = 169441

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE customer_data_1_rwd LOCATION 'dbfs:/mnt/prod-enterprise_data/bronze/delta/customer_data_1_rwd_prod.db/'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ext_rwd;
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ext_rwd.ext_diagnosis_code_otl

# COMMAND ----------


