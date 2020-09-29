# Databricks notebook source
"""
Use the userprincipalname() function inb the DAX expression for a filter in the report
https://docs.microsoft.com/en-us/power-bi/admin/service-admin-rls#:~:text=Row%2Dlevel%20security%20(RLS)%20with%20Power%20BI%20can%20be,to%20datasets%20in%20the%20workspace.

dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
"""

# COMMAND ----------

departure_delays = spark.read.option("inferSchema", "true").option("header", "true").csv("/databricks-datasets/flights/departuredelays.csv")
departure_delays.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save("/datasets/delta/flights/departure_delays")

airport_codes_na = sc.textFile("/databricks-datasets/flights/airport-codes-na.txt").map(lambda x: x.split("\t")).toDF(["City", "State", "Country", "IATA"]).filter("City <> 'City'")
airport_codes_na.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save("/datasets/delta/flights/airport_codes_na")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE departure_delays
# MAGIC USING DELTA
# MAGIC LOCATION "/datasets/delta/flights/departure_delays/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE airport_codes
# MAGIC USING DELTA
# MAGIC LOCATION "/datasets/delta/flights/airport_codes_na/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW vw_airport_codes_dest AS SELECT * FROM airport_codes

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE security_example

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE security_example AS 
# MAGIC SELECT 'IL' AS principal, 'Administrator@STATEOFTHEARTZ.IO' AS user UNION 
# MAGIC SELECT 'AL' AS principal, 'michael.artz@databricks.com' AS user UNION 
# MAGIC SELECT 'AK' AS principal, 'michael.artz@databricks.com' AS user UNION 
# MAGIC SELECT 'AZ' AS principal, 'Administrator@STATEOFTHEARTZ.IO' AS user

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE security_example_2 AS 
# MAGIC SELECT 'ORD' AS principal, 'Administrator@STATEOFTHEARTZ.IO' AS user UNION 
# MAGIC SELECT 'ORD' AS principal, 'michael.artz@databricks.com' AS user UNION 
# MAGIC SELECT 'IAH' AS principal, 'michael.artz@databricks.com' AS user UNION 
# MAGIC SELECT 'LAX' AS principal, 'michael.artz@databricks.com' AS user UNION 
# MAGIC SELECT 'DEN' AS principal, 'michael.artz@databricks.com' AS user UNION 
# MAGIC SELECT 'TPA' AS principal, 'michael.artz@databricks.com' AS user UNION 
# MAGIC SELECT 'SFO' AS principal, 'Administrator@STATEOFTHEARTZ.IO' AS user

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO security_example_2
# MAGIC SELECT 'ANC' AS principal, 'scott.love@databricks.com' AS user UNION 
# MAGIC SELECT 'PHX' AS principal, 'scott.love@databricks.com' AS user 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT aco.City as o_city
# MAGIC   , aco.State as o_state
# MAGIC   , acd.City as d_city
# MAGIC   , acd.State as d_state
# MAGIC   , dd.date
# MAGIC   , dd.delay
# MAGIC   , dd.distance
# MAGIC   , s.user
# MAGIC FROM departure_delays dd
# MAGIC JOIN airport_codes aco ON aco.iata = dd.origin
# MAGIC JOIN airport_codes acd ON acd.iata = dd.destination
# MAGIC JOIN security_example s ON s.principal = aco.state 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT aco.City as o_city
# MAGIC   , aco.State as o_state
# MAGIC   , acd.City as d_city
# MAGIC   , acd.State as d_state
# MAGIC   , dd.date
# MAGIC   , dd.delay
# MAGIC   , dd.distance
# MAGIC   , collect_set(s.user) as users
# MAGIC   , aco.IATA
# MAGIC FROM departure_delays dd
# MAGIC JOIN airport_codes aco ON aco.iata = dd.origin
# MAGIC JOIN airport_codes acd ON acd.iata = dd.destination
# MAGIC JOIN security_example s ON s.principal = aco.state 
# MAGIC WHERE aco.state IN ('AL', 'AK', 'AZ', 'IL', 'WI', 'TN')
# MAGIC GROUP BY aco.City
# MAGIC   , aco.State 
# MAGIC   , acd.City
# MAGIC   , acd.State
# MAGIC   , dd.date
# MAGIC   , dd.delay
# MAGIC   , dd.distance
# MAGIC   , aco.IATA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.IATA
# MAGIC     , count(*) as code_count
# MAGIC FROM airport_codes a
# MAGIC GROUP BY a.IATA 
# MAGIC ORDER BY code_count DESC

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/flights/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM security_example
