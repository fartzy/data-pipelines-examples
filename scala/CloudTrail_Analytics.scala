// Databricks notebook source
// MAGIC %md # Analyzing Structured Streaming in CloudTrail
// MAGIC In this notebook, we take our streaming table that was created from the the cloud trail logs in the notebook [CloudTrail ETL Analytics](https://acme.cloud.databricks.com/#notebook/999999/command/213891)

// COMMAND ----------

// MAGIC %md
// MAGIC We can use the location of the parquet files in **/tmp/cloudTrailRaw/** to create a new tempTable for analysis

// COMMAND ----------

val parquetOutputPath = "/tmp/cloudTrailRaw/"  // DBFS or S3 path
sql(s"select * from parquet.`$parquetOutputPath`").createOrReplaceTempView("cloudTrailRaw")

// COMMAND ----------

// DBTITLE 1,Total Event Type By Month
// MAGIC %sql
// MAGIC SELECT YEAR(eventTime) || "-" || MONTH(eventTime) As `month`
// MAGIC      , eventType
// MAGIC      , COUNT(*) as rowCount
// MAGIC FROM cloudTrailRaw
// MAGIC WHERE 1=1
// MAGIC GROUP BY eventType
// MAGIC      , YEAR(eventTime) || "-" || MONTH(eventTime)

// COMMAND ----------

// DBTITLE 1,All AWS Console Sign Ins
// MAGIC %sql
// MAGIC SELECT eventTime
// MAGIC     , userIdentity['sessionContext']['sessionIssuer']['userName'] AS userName
// MAGIC     , responseElements['ConsoleLogin'] AS result
// MAGIC     , awsRegion
// MAGIC FROM cloudTrailRaw
// MAGIC WHERE eventType = 'AwsConsoleSignIn'
// MAGIC ORDER BY eventTime DESC

// COMMAND ----------

// DBTITLE 1,AWS APICalls Per User Through Sign Ins
val userAccessFromConsoleDf = sql(s"""
  SELECT userIdentity['userName'] as UserName
      , MAX(eventTime) As maxTime
      , MIN(eventTime) As minTime
      , COUNT(*) as rowCount
      , MAX(eventSource) maxEventSource
      , MIN(eventSource) minEventSource
  FROM cloudTrailRaw
  WHERE 1=1
    AND userIdentity['userName'] IS NOT NULL
  GROUP BY userIdentity['userName']
  ORDER BY rowCount DESC
  """)
display(userAccessFromConsoleDf)

// COMMAND ----------

// DBTITLE 1,Most Recent And Oldest AWS Console Login Per Biogen User
val mostRecentLoginDf = sql(s"""
  SELECT userIdentity['userName'] AS userName
      , MAX(eventTime) As maxTime
      , MIN(eventTime) As minTime
      , COUNT(*) as rowCount
  FROM cloudTrailRaw
  WHERE 1=1
    AND userIdentity['userName'] IS NOT NULL
  GROUP BY userIdentity['userName']
  """)
display(mostRecentLoginDf)


// COMMAND ----------

// DBTITLE 1,All Glue Calls
// MAGIC %sql
// MAGIC SELECT eventTime
// MAGIC     , userIdentity['userName'] AS userName
// MAGIC     , userIdentity['accountID'] AS userAccountID
// MAGIC     , eventName
// MAGIC     , eventSource
// MAGIC     , resources["ARN"] AS amazonResourceNum
// MAGIC     , requestParameters
// MAGIC     , resources
// MAGIC     , errorCode || ": " || errorMessage AS error
// MAGIC FROM cloudTrailRaw
// MAGIC WHERE 1=1
// MAGIC   AND eventSource = 'glue.amazonaws.com'
// MAGIC ORDER BY eventTime DESC

// COMMAND ----------

// DBTITLE 1,S3 Access Denied From Databricks Query
// MAGIC %sql
// MAGIC SELECT eventTime
// MAGIC      , requestParameters['bucketName'] AS bucketName
// MAGIC      , requestParameters['key'] AS Resource
// MAGIC      , eventName
// MAGIC      , awsRegion
// MAGIC      , errorCode
// MAGIC FROM cloudTrailRaw
// MAGIC WHERE 1=1
// MAGIC  AND eventSource = 's3.amazonaws.com'
// MAGIC  AND userIdentity['accountId'] = '08931199XXXX'
// MAGIC  AND errorCode IS NOT NULL
// MAGIC  AND errorCode <> 'NoSuchKey'
// MAGIC ORDER BY eventTime DESC

// COMMAND ----------

// DBTITLE 1,All Writes to S3 From Databricks
// MAGIC %sql
// MAGIC SELECT eventTime
// MAGIC      , requestParameters['bucketName'] AS bucketName
// MAGIC      , requestParameters['key'] AS Resource
// MAGIC      , eventName
// MAGIC      , awsRegion
// MAGIC FROM cloudTrailRaw
// MAGIC WHERE 1=1
// MAGIC  AND eventSource = 's3.amazonaws.com'
// MAGIC  AND userIdentity['accountId'] = '08931199XXXX'
// MAGIC  AND eventName = 'PutObject'
// MAGIC ORDER BY eventTime DESC

// COMMAND ----------

// DBTITLE 1,All AWS API calls with Databricks Account
// MAGIC %sql
// MAGIC SELECT eventTime
// MAGIC     , userIdentity['userName'] AS userName
// MAGIC     , userIdentity['accountID'] AS userAccountID
// MAGIC     , eventName
// MAGIC     , eventSource
// MAGIC     , resources["ARN"] AS amazonResourceNum
// MAGIC     , requestParameters['bucketName'] AS bucketName
// MAGIC     , requestParameters['prefix'] AS s3Folder
// MAGIC     , errorCode || ": " || errorMessage AS error
// MAGIC FROM cloudTrailRaw
// MAGIC WHERE 1=1
// MAGIC   AND userIdentity['accountID'] = '08931199XXXX'
// MAGIC ORDER BY eventTime DESC

// COMMAND ----------

// DBTITLE 1,All AWS S3 Actions From Databricks
// MAGIC %sql
// MAGIC SELECT eventTime
// MAGIC     , userIdentity['userName'] AS userName
// MAGIC     , userIdentity['accountID'] AS userAccountID
// MAGIC     , eventName
// MAGIC     , eventSource
// MAGIC     , resources["ARN"] AS `amazonResourceNumber(s)`
// MAGIC     , requestParameters['bucketName'] AS bucketName
// MAGIC     , requestParameters['prefix'] AS s3Folder
// MAGIC     , errorCode || ": " || errorMessage AS error
// MAGIC FROM cloudTrailRaw
// MAGIC WHERE 1=1
// MAGIC   AND userIdentity['accountID'] = '08931199XXXX'
// MAGIC   AND eventSource = 's3.amazonaws.com'
// MAGIC ORDER BY eventTime DESC

// COMMAND ----------

// DBTITLE 1,All AWS Non-S3 Actions From Databricks
// MAGIC %sql
// MAGIC SELECT eventTime
// MAGIC     , userIdentity['userName'] AS userName
// MAGIC     , userIdentity['accountID'] AS userAccountID
// MAGIC     , eventName
// MAGIC     , eventSource
// MAGIC     , resources["ARN"] AS amazonResourceNum
// MAGIC     , requestParameters
// MAGIC     , resources
// MAGIC     , errorCode || ": " || errorMessage AS error
// MAGIC FROM cloudTrailRaw
// MAGIC WHERE 1=1
// MAGIC   AND userIdentity['accountID'] = '08931199XXXX'
// MAGIC   AND eventSource <> 's3.amazonaws.com'
// MAGIC ORDER BY eventTime DESC

// COMMAND ----------

// DBTITLE 1,Total CloudTrail Record Count
// MAGIC %sql
// MAGIC SELECT COUNT(*)
// MAGIC FROM cloudTrailRaw
// MAGIC --16078083

// COMMAND ----------

// DBTITLE 1,Total CloudTrail Record Count Per Date
// MAGIC %sql
// MAGIC SELECT COUNT(*)
// MAGIC     , date
// MAGIC FROM cloudTrailRaw
// MAGIC GROUP BY date

// COMMAND ----------

// DBTITLE 1,Output Data
// MAGIC %python
// MAGIC from datetime import datetime
// MAGIC today = datetime.today().strftime("%Y-%m-%d")
// MAGIC today_df = sql("select * from cloudTrailRaw where date_format(timestamp,'yyyy-MM-dd') = {today} limit 10".format(today=today))
// MAGIC dbutils.widgets.text("report_path", "/usage_reporting/executions")
// MAGIC root_path = dbutils.widgets.get("report_path")
// MAGIC report_path = (root_path + "/bu=all-cloudtrail/" + today).lower()
// MAGIC dbutils.fs.mkdirs("{report_path}".format(report_path=report_path))
// MAGIC today_df.coalesce(1).write.mode("overwrite").format("parquet").save(report_path + "/report_data")
// MAGIC print("cumulativeDF written to {report_path}/report_data/".format(report_path=report_path))
