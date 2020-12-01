# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://www.logolynx.com/images/logolynx/96/96b6d740c8d398aeb20d3a02adefd908.png" alt="Databricks Customer Usage" style="width: 600px;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Welcome to the Databricks Usage Analytics Dashboard
# MAGIC This dashboard leverages the usage data sent directly to your designated object store. Please carefully follow the instructions below in order to execute a daily refresh of each chart and report.
# MAGIC 
# MAGIC #### How to use this notebook
# MAGIC 1. Please enter your object store (bucket) name in the 'Usage File Path' string below, in addition to the host URL of your workspace as indicated in Cmd 3, below. The syntax has been provided for your reference. Note that in cases where this notebook has been shared, this may already be done for you.
# MAGIC 2. You may now click Run All at the top of this notebook. Note that you may need to enter any sku (automated, interactive) pricing or commit dollars per your contract.

# COMMAND ----------

# # Insert usage file path and host URL below. Example: 's3a://my-bucket/delivery/path/csv/dbus/'
# usagefilePath = "s3a://cse-shard-usage/databricks-usage/csv/dbus/"
usagefilePath = "/mnt/logs/db-usage-stats"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Please do not modify the code below. It is strictly for the operation of this notebook.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

"""
Widgets
"""
dbutils.widgets.dropdown("Top N Usage", 'Top 10', ['Top 5', 'Top 10', 'Top 15', 'Top 20', 'Top 25', 'Top 30', 'All'])
dbutils.widgets.multiselect("Attribute Grouping", "Cluster Name", ['Cluster Name', 'Cluster Owner User Name'])
dbutils.widgets.dropdown("Date Grouping", 'Day', ['Day', 'Week', 'Month', 'Year'])
dbutils.widgets.text("Cluster Owner User Name", "")
dbutils.widgets.text("BusinessUnit", "")
dbutils.widgets.text("Team", "")
dbutils.widgets.text("report_path", "/dbfs/tmp/usage_report/")

"""
Build usage schema
"""
usageSchema = StructType([
  StructField("workspaceId", StringType(), False),
  StructField("timestamp", DateType(), False),
  StructField("clusterId", StringType(), False),
  StructField("clusterName", StringType(), False),
  StructField("clusterNodeType", StringType(), False),
  StructField("clusterOwnerUserId", StringType(), False),
  StructField("clusterCustomTags", StringType(), False),
  StructField("sku", StringType(), False),
  StructField("dbus", FloatType(), False),
  StructField("machineHours", FloatType(), False),
  StructField("clusterOwnerUserName", StringType(), False)
])

# Instantiate and cache the usage dataframe
df = (spark.read
      .option("header", "true")
      .option("escape", "\"")
      .schema(usageSchema)
      .csv(usagefilePath)
      )

usageDF = (df.select('workspaceId',
                     'timestamp',
                     'clusterId',
                     'clusterName',
                     'clusterNodeType',
                     'clusterOwnerUserId',
                     from_json('clusterCustomTags', MapType(StringType(), StringType())).alias("clusterCustomTags"),
                     when(col('sku') == 'STANDARD_INTERACTIVE_OPSEC', 'Interactive')
                     .when(col('sku') == 'STANDARD_AUTOMATED_NON_OPSEC', 'Automated')
                     .when(col('sku') == 'STANDARD_INTERACTIVE_NON_OPSEC', 'Interactive')
                     .when(col('sku') == 'LIGHT_AUTOMATED_NON_OPSEC', 'Automated Light')
                     .when(col('sku') == 'STANDARD_AUTOMATED_OPSEC', 'Automated')
                     .when(col('sku') == 'LIGHT_AUTOMATED_OPSEC', 'Automated Light')
                     .otherwise(col('sku')),
                     'dbus',
                     'machineHours',
                     'clusterOwnerUserName')
                     .withColumnRenamed('CASE WHEN (sku = STANDARD_INTERACTIVE_OPSEC) THEN Interactive WHEN (sku = STANDARD_AUTOMATED_NON_OPSEC) THEN Automated WHEN (sku = STANDARD_INTERACTIVE_NON_OPSEC) THEN Interactive WHEN (sku = LIGHT_AUTOMATED_NON_OPSEC) THEN Automated Light WHEN (sku = STANDARD_AUTOMATED_OPSEC) THEN Automated WHEN (sku = LIGHT_AUTOMATED_OPSEC) THEN Automated Light ELSE sku END', 'sku')
           .cache()
          )


#trim to the specific Business Unit 
#'ALL' or '' for queries to be run all business Units
bu = getArgument("BusinessUnit")
if bu not in ['ALL','']:
  usageDF = usageDF.filter(col("clusterCustomTags.BusinessUnit") == bu)
  

team = getArgument("Team")
if team not in ['ALL','']:
  usageDF = usageDF.filter(col("clusterCustomTags.Team") == team)  
  
# Create temp table for sql commands
usageDF.createOrReplaceTempView("usage")

# Check to see if the query returned results, and if so, display them.
try:    
  assert usageDF.count() == 0
  print("Your query returned no results. This is because there no BusinessUnits of '{bu}' or no Teams of '{team}' in data.".format(bu=bu, team=team))
  dbutils.notebook.exit("Exiting Job as there is no data for the given job parameters of BusinessUnit='{bu}' and Team='{team}'")
except:
  pass

# COMMAND ----------

# Instantiate Widgets for dynamic filtering
import datetime
# dbutils.widgets.removeAll()

# Commit Amount
dbutils.widgets.text("Commit Dollars", "00.00")
commit = getArgument("Commit Dollars")

# Sku Pricing. Creates a text widget for each distinct value in a customer's account.
skus = spark.sql("select distinct(sku) from usage").rdd.map(lambda row : row[0]).collect()
for sku in skus:  # Print text boxes for each distinct customer sku
  dbutils.widgets.text(("{}").format("SKU Price - " + sku), ".00")

# Sku Filter
defaultSku = skus[0]
dbutils.widgets.multiselect("Sku", defaultSku, [str(sku) for sku in skus])

# Tag Filter
tags = spark.sql("select distinct(explode(map_keys(clusterCustomTags))) from usage").rdd.map(lambda row : row[0]).collect()
if len(tags) > 0:
  defaultTag = tags[0]
  dbutils.widgets.multiselect("Cluster Tag Name", defaultTag, [str(x) for x in tags])

# Date Window Filter
now = datetime.datetime.now()
dbutils.widgets.text("Date - End", now.strftime("%Y-%m-%d"))
dbutils.widgets.text("Date - Beginning", now.strftime("%Y-%m-%d"))

# Create a dataframe from sku names and their rates. This gets joined to the usage dataframe to get spend.
skuVals = [str(sku) for sku in skus]
wigVals = [getArgument("SKU Price - " + sku) for sku in skus]

skuZip = list(zip(skuVals, wigVals)) # Create a list object from each sku and its corresponding rate to parallelize into an RDD

skuRdd = sc.parallelize(skuZip) # Create RDD

skuSchema = StructType([
  StructField("sku", StringType(), True),
  StructField("rate", StringType(), True)])

skuDF = spark.createDataFrame(skuRdd, skuSchema) # Create a dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Cumulative Reporting
# MAGIC 1. Cumulative Daily Spend ($) vs Commit
# MAGIC 2. Cumulative Daily Spend ($) by Tag

# COMMAND ----------

# DBTITLE 1,Cumulative Daily Spend ($) vs Commit
# Generate month usage dataframe
from pyspark.sql.window import Window
import pyspark.sql.functions as func

# Window function to get cumulative spend. This is added to the usageDF object.
cumulativeUsage = Window \
.orderBy("Date") \
.rowsBetween(Window.unboundedPreceding, 0)

daySum = func.sum(func.col("Daily Spend")).over(cumulativeUsage)

cumulativeDF = (usageDF
           .filter((usageDF.timestamp.between((getArgument("Date - Beginning")), (getArgument("Date - End")))))
           .join(skuDF, "sku")
           .withColumn("Spend", usageDF['dbus'] * skuDF['rate'])
           .withColumn("Commit", lit(getArgument("Commit Dollars")))
           .select((date_format('timestamp', 'yyyy-MM-dd').alias('Date')), 'Spend', 'dbus', 'Commit')
           .groupBy('Date', 'Commit')
           .sum('dbus', 'Spend')
           .withColumnRenamed('sum(dbus)', 'Total DBUs')
           .withColumnRenamed('sum(spend)', 'Daily Spend')
           .withColumn("Cumulative Spend", daySum)
           .orderBy('Date')
          )

# Check to see if the query returned results, and if so, display them.
try:
  assert cumulativeDF.count() == 0
  print("Your query returned no results. This could be based on selected filters.")
except:
  display(cumulativeDF)

# COMMAND ----------

# DBTITLE 1,Top 10 Cluster Spend ($) By Month
skuDF.createOrReplaceTempView("sku_vw")
date_widget_val = getArgument("Date Grouping")
attribs = "Cluster Name"
date_filter = "AND DATE(timestamp) BETWEEN '" + getArgument("Date - Beginning") + "' AND '" + getArgument("Date - End") + "' "
limit_top_n = ("Limit 10")
cluster_owner_name = getArgument("Cluster Owner User Name")

select_date = date_widget_val \
  .replace("Year", "YEAR(DATE(timestamp))  AS `Year`") \
  .replace("Month", "FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp, 'yyyy-MM-dd'), 'MMMMM') AS `Month`") \
  .replace("Week", "WEEKOFYEAR(DATE(timestamp)) AS `Week`") \
  .replace("Day", "DATE(timestamp) As `Day`")

date_group = date_widget_val \
  .replace("Year", "YEAR(DATE(timestamp))") \
  .replace("Month", "FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp, 'yyyy-MM-dd'), 'MMMMM')") \
  .replace("Week", "WEEKOFYEAR(DATE(timestamp))") \
  .replace("Day", "DATE(timestamp)")

select_attrib = attribs \
  .replace("Cluster Owner", "REPLACE(clusterOwnerUserName, '@acme.com','') AS `Cluster Owner`") \
  .replace("Cluster Name", "clusterName AS `Cluster Name`")

attrib_group = attribs \
  .replace("Cluster Owner", "REPLACE(clusterOwnerUserName, '@acme.com','')") \
  .replace("Cluster Name", "clusterName")

if cluster_owner_name == "":
  cluster_owner_filter = "" 
else:
  cluster_owner_filter = "AND clusterOwnerUserName IN (" + ",".join(["'" + s.strip() + "'" for s in getArgument("Cluster Owner User Name").split(",")]) + ") "
  
  
query_string = """
 SELECT SUM(dbus) AS `Total DBUs`
       , SUM(dbus * rate) AS `($) Per {date_widget_val}, {attribs}`
       , {select_attrib}
       , {select_date}
  FROM usage u
   JOIN sku_vw s ON u.sku = s.sku 
  WHERE 1 = 1
    {date_filter}
    {cluster_owner_filter}
  GROUP BY {attrib_group}
         , {date_group}
  ORDER BY `($) Per {date_widget_val}, {attribs}` DESC
  {limit_top_n}
""".format(attrib_group=attrib_group, date_group=date_group, date_filter=date_filter, cluster_owner_filter=cluster_owner_filter
          , select_date=select_date, select_attrib=select_attrib, limit_top_n=limit_top_n, attribs=attribs, date_widget_val=date_widget_val)

#print(query_string)
slicedDF = spark.sql(query_string)

display(slicedDF)

# COMMAND ----------

# DBTITLE 1,Top 25 Cluster Owner Spend ($) By Month
date_widget_val = getArgument("Date Grouping")
attribs = dbutils.widgets.get("Attribute Grouping").replace("Cluster Owner User Name","Cluster Owner")
date_filter = "AND DATE(timestamp) BETWEEN '" + getArgument("Date - Beginning") + "' AND '" + getArgument("Date - End") + "' "
limit_top_n = ("Limit 25")
cluster_owner_name = getArgument("Cluster Owner User Name")

select_date = date_widget_val \
  .replace("Year", "YEAR(DATE(timestamp))  AS `Year`") \
  .replace("Month", "FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp, 'yyyy-MM-dd'), 'MMMMM') AS `Month`") \
  .replace("Week", "WEEKOFYEAR(DATE(timestamp)) AS `Week`") \
  .replace("Day", "DATE(timestamp) As `Day`")

date_group = date_widget_val \
  .replace("Year", "YEAR(DATE(timestamp))") \
  .replace("Month", "FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp, 'yyyy-MM-dd'), 'MMMMM')") \
  .replace("Week", "WEEKOFYEAR(DATE(timestamp))") \
  .replace("Day", "DATE(timestamp)")

select_attrib = attribs \
  .replace("Cluster Owner", "REPLACE(clusterOwnerUserName, '@acme.com','') AS `Cluster Owner`") \
  .replace("Cluster Name", "clusterName AS `Cluster Name`")

attrib_group = attribs \
  .replace("Cluster Owner", "REPLACE(clusterOwnerUserName, '@acme.com','')") \
  .replace("Cluster Name", "clusterName")

if cluster_owner_name == "":
  cluster_owner_filter = "" 
else:
  cluster_owner_filter = "AND clusterOwnerUserName IN (" + ",".join(["'" + s.strip() + "'" for s in getArgument("Cluster Owner User Name").split(",")]) + ") "
  
  
query_string = """
 SELECT SUM(dbus) AS `Total DBUs`
       , SUM(dbus * rate) AS `($) Per {date_widget_val}, {attribs}`
       , {select_attrib}
       , {select_date}
  FROM usage u
   JOIN sku_vw s ON u.sku = s.sku 
  WHERE 1 = 1
    {date_filter}
    {cluster_owner_filter}
  GROUP BY {attrib_group}
         , {date_group}
  ORDER BY `($) Per {date_widget_val}, {attribs}` DESC
  {limit_top_n}
""".format(attrib_group=attrib_group, date_group=date_group, date_filter=date_filter, cluster_owner_filter=cluster_owner_filter
          , select_date=select_date, select_attrib=select_attrib, limit_top_n=limit_top_n, attribs=attribs, date_widget_val=date_widget_val)

#print(query_string)
slicedDF = spark.sql(query_string)

display(slicedDF)

# COMMAND ----------

# DBTITLE 1,INTERACTIVE Spend ($) Grouped by Cluster & Cluster Owner & Date
skuDF.createOrReplaceTempView("sku_vw")
date_widget_val = getArgument("Date Grouping")
attribs = dbutils.widgets.get("Attribute Grouping").replace("Cluster Owner User Name","Cluster Owner")
date_filter = "AND DATE(timestamp) BETWEEN '" + getArgument("Date - Beginning") + "' AND '" + getArgument("Date - End") + "' "
limit_top_n = getArgument("Top N Usage").replace("Top", "Limit").replace("All", "")
cluster_owner_name = getArgument("Cluster Owner User Name")

select_date = date_widget_val \
  .replace("Year", "YEAR(DATE(timestamp))  AS `Year`") \
  .replace("Month", "FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp, 'yyyy-MM-dd'), 'MMMMM') AS `Month`") \
  .replace("Week", "WEEKOFYEAR(DATE(timestamp)) AS `Week`") \
  .replace("Day", "DATE(timestamp) As `Day`")

date_group = date_widget_val \
  .replace("Year", "YEAR(DATE(timestamp))") \
  .replace("Month", "FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp, 'yyyy-MM-dd'), 'MMMMM')") \
  .replace("Week", "WEEKOFYEAR(DATE(timestamp))") \
  .replace("Day", "DATE(timestamp)")

select_attrib = attribs \
  .replace("Cluster Owner", "REPLACE(clusterOwnerUserName, '@acme.com','') AS `Cluster Owner`") \
  .replace("Cluster Name", "clusterName AS `Cluster Name`")

attrib_group = attribs \
  .replace("Cluster Owner", "REPLACE(clusterOwnerUserName, '@acme.com','')") \
  .replace("Cluster Name", "clusterName")

if cluster_owner_name == "":
  cluster_owner_filter = "" 
else:
  cluster_owner_filter = "AND clusterOwnerUserName IN (" + ",".join(["'" + s.strip() + "'" for s in getArgument("Cluster Owner User Name").split(",")]) + ") "
  
  
query_string = """
 SELECT SUM(dbus) AS `Total DBUs`
       , SUM(dbus * rate) AS `($) Per {date_widget_val}, {attribs}`
       , {select_attrib}
       , {select_date}
  FROM usage u
   JOIN sku_vw s ON u.sku = s.sku 
  WHERE 1 = 1
    {date_filter}
    {cluster_owner_filter}
  GROUP BY {attrib_group}
         , {date_group}
  ORDER BY `($) Per {date_widget_val}, {attribs}` DESC
  {limit_top_n}
""".format(attrib_group=attrib_group, date_group=date_group, date_filter=date_filter, cluster_owner_filter=cluster_owner_filter
          , select_date=select_date, select_attrib=select_attrib, limit_top_n=limit_top_n, attribs=attribs, date_widget_val=date_widget_val)

#print(query_string)
slicedDF = spark.sql(query_string)

display(slicedDF)

# COMMAND ----------

# DBTITLE 1,Cumulative Daily Spend ($) by Tag
# Create daily usage dataframe. 
if not tags:
  print("No tags recorded during this time range, skipping...")
else:
  # Window function to get cumulative spend. This is added to the usageDF object.
  cumulativeUsageByTag = Window \
  .partitionBy("Tag Name", "Tag Value") \
  .orderBy("Date") \
  .rowsBetween(Window.unboundedPreceding, 0)

  daySumByTag = func.sum(func.col("Daily Spend")).over(cumulativeUsageByTag)

  tagDF = (usageDF
           .filter((usageDF.timestamp.between((getArgument("Date - Beginning")), (getArgument("Date - End")))))
           .join(skuDF, "sku")
           .withColumn("Spend", usageDF['dbus'] * skuDF['rate'])
           .filter(array_contains(map_keys(usageDF["clusterCustomTags"]), getArgument("Cluster Tag Name")))
           .withColumn("tagName", lit(getArgument("Cluster Tag Name")))
           .withColumn("tagValue", usageDF["clusterCustomTags." + getArgument("Cluster Tag Name")])
           .select('Spend', 'tagName', 'timestamp', 'tagValue')
           .withColumnRenamed('timestamp', 'Date')
           .groupBy('tagName', 'tagValue', 'Date')
           .sum('Spend')
           .withColumnRenamed('sum(spend)', 'Daily Spend')
           .withColumnRenamed('tagName', 'Tag Name')
           .withColumnRenamed('tagValue', 'Tag Value')
           .withColumn("Cumulative Spend", daySumByTag)
           .orderBy('Date')
           )  
  # Check to see if the query returned results, and if so, display them.
  try:    
    assert tagDF.count() == 0
    print("Your query returned no results. This could be based on selected filters.")
  except:
    display(tagDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Daily Trend Reports
# MAGIC 1. Daily Spend
# MAGIC 2. Daily DBUs by SKU
# MAGIC 3. Daily Usage or Spend by Cluster Owner

# COMMAND ----------

# DBTITLE 1,Daily Spend ($)
# Create daily usage dataframe. 
dailyDF = (usageDF
           .filter((usageDF.timestamp.between((getArgument("Date - Beginning")), (getArgument("Date - End")))))
           .join(skuDF, "sku")
           .withColumn("Spend", usageDF['dbus'] * skuDF['rate'])
           .select('timestamp', 'Spend', 'sku', 'dbus')
           .withColumnRenamed('timestamp', 'Date')
           .groupBy('Date')
           .sum('Spend')
           .withColumnRenamed('sum(spend)', 'Total Spend')
           .orderBy('Date')
          )

# Check to see if the query returned results, and if so, display them.
try:
  assert dailyDF.count() == 0
  print("Your query returned no results. This could be based on selected filters.")
except:
  display(dailyDF)

# COMMAND ----------

# DBTITLE 1,Daily DBUs By SKU
# Create daily usage dataframe. 
dailyskuDF = (usageDF
              .filter((usageDF.timestamp.between((getArgument("Date - Beginning")), (getArgument("Date - End")))))
              .join(skuDF, "sku")
              .withColumn("Spend", usageDF['dbus'] * skuDF['rate'])
              .select('timestamp', 'Spend', 'sku', 'dbus')
              .withColumnRenamed('timestamp', 'Date')
              .groupBy('sku', 'Date')
              .sum('dbus')
              .withColumnRenamed('sum(dbus)', 'Total DBUs')
              .withColumnRenamed('sku', 'Sku')
              .orderBy('Date')
              )

# Check to see if the query returned results, and if so, display them.
try:
  assert dailyskuDF.count() == 0
  print("Your query returned no results. This could be based on selected filters.")
except:
  display(dailyskuDF)

# COMMAND ----------

# DBTITLE 1,Daily Usage (DBU) or Spend ($) by Cluster Owner
# Create daily usage dataframe. 
userDF = (usageDF
           .filter((usageDF.timestamp.between((getArgument("Date - Beginning")), (getArgument("Date - End")))))
           .join(skuDF, "sku")
           .withColumn("Spend", usageDF['dbus'] * skuDF['rate'])
           .select('timestamp', 'Spend', 'sku', 'dbus', 'clusterOwnerUserName')
           .withColumnRenamed('timestamp', 'Date')
           .withColumnRenamed('sku', 'Sku')
           .withColumnRenamed('clusterOwnerUserName', 'Cluster Owner')
           .groupBy('Sku', 'Date', 'Cluster Owner')
           .sum('dbus', 'Spend')
           .withColumnRenamed('sum(dbus)', 'Total DBUs')
           .withColumnRenamed('sum(Spend)', 'Total Spend')
           .orderBy('Date')
          )

# Check to see if the query returned results, and if so, display them.
try:
  assert userDF.count() == 0
  print("Your query returned no results. This could be based on selected filters.")
except:
  display(userDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Monthly Trend Reports
# MAGIC 1. Monthly Spend
# MAGIC 2. Monthly DBUs by SKU
# MAGIC 3. Daily Usage and Spend by Cluster Owner
# MAGIC 4. Automated DBUs by Job ID
# MAGIC 5. Interactive DBUs by Cluster Name

# COMMAND ----------

# DBTITLE 1,Monthly Spend ($)
# Create daily usage dataframe. 
monthspendDF = (usageDF
                .filter((usageDF.timestamp.between((getArgument("Date - Beginning")), (getArgument("Date - End")))))
                .join(skuDF, "sku")
                .withColumn("Spend", usageDF['dbus'] * skuDF['rate'])
                .withColumn("Month", date_format(to_date('timestamp'), 'yyyy-MM'))
                .select('Month', 'Spend', 'sku', 'dbus')
                .groupBy('Month')
                .sum('Spend')
                .withColumnRenamed('sum(Spend)', 'Total Spend')
                .withColumnRenamed('sku', 'Sku')
                .orderBy('Month')
                )

# Check to see if the query returned results, and if so, display them.
try:
  assert monthspendDF.count() == 0
  print("Your query returned no results. This could be based on selected filters.")
except:
  display(monthspendDF)

# COMMAND ----------

# DBTITLE 1,Monthly DBUs by SKU
# Create daily usage dataframe. 
monthDF = (usageDF
           .filter((usageDF.timestamp.between((getArgument("Date - Beginning")), (getArgument("Date - End")))))
           .join(skuDF, "sku")
           .withColumn("Spend", usageDF['dbus'] * skuDF['rate'])
           .withColumn("Month", date_format(to_date('timestamp'), 'yyyy-MM'))
           .select('Month', 'Spend', 'sku', 'dbus')
           .groupBy('sku', 'Month')
           .sum('dbus')
           .withColumnRenamed('sum(dbus)', 'Total DBUs')
           .withColumnRenamed('sku', 'Sku')
           .orderBy('Month')
          )

# Check to see if the query returned results, and if so, display them.
try:
  assert monthDF.count() == 0
  print("Your query returned no results. This could be based on selected filters.")
except:
  display(monthDF)

# COMMAND ----------

# DBTITLE 1,Automated DBUs by Job ID
clusterDF = (usageDF.select('dbus', 'clusterName')
             .filter((usageDF.timestamp.between((getArgument("Date - Beginning")), (getArgument("Date - End")))))
             .withColumn("JobId", regexp_extract(df.clusterName, 'job-(\\d+)-run-\\d+', 1))
             .withColumnRenamed("clusterName", "Cluster Name")
             .withColumnRenamed("JobId", "Job ID")
             .filter(usageDF.sku == lit("Automated"))
             .groupBy("Job ID")
             .sum('dbus')
             .withColumnRenamed("sum(dbus)", "Total Automated DBUs")
             .orderBy(desc('Total Automated DBUs'))
            )

# Check to see if the query returned results, and if so, display them.
try:
  assert clusterDF.count() == 0
  print("Your query returned no results. This could be based on selected filters.")
except:
  display(clusterDF)

# COMMAND ----------

# DBTITLE 1,Interactive DBUs by Cluster Name
interactiveDF = (usageDF.select('dbus', 'timestamp', 'clusterName')
                 .filter((usageDF.timestamp.between((getArgument("Date - Beginning")), (getArgument("Date - End")))))
                 .withColumn("Month", date_format(to_date('timestamp'), 'yyyy-MM'))
                 .withColumnRenamed('clusterName', 'Cluster')
                 .filter(usageDF.sku == lit("Interactive"))
                 .groupBy('Cluster', 'Month')
                 .sum('dbus').withColumnRenamed('sum(dbus)', 'Total Interactive DBUs')
                 .orderBy('Month')
                )

# Check to see if the query returned results, and if so, to display them.
try:
  assert interactiveDF.count() == 0
  print("Your query returned no results. This could be based on selected filters.")
except:
  display(interactiveDF)

# COMMAND ----------

# DBTITLE 1,Output Data 
from datetime import datetime
bu = dbutils.widgets.get("BusinessUnit")
root_path = dbutils.widgets.get("report_path")
date=datetime.today().strftime('%Y-%m-%d')
report_path = (root_path + "/bu={bu}".format(bu=bu) + datetime.today().strftime('%Y-%m-%d')).lower()
dbutils.fs.mkdirs("{report_path}".format(report_path=report_path))
cumulativeDF.coalesce(1).write.mode("overwrite").format("csv").save(report_path + "/report_data")
print("{bu}_{date}_data written to {report_path}/report_data/".format(
  report_path=report_path, date=date, bu=bu
))
