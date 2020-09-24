// Databricks notebook source
// MAGIC %md # Streaming ETL on CloudTrail Logs using Structured Streaming
// MAGIC
// MAGIC
// MAGIC [AWS CloudTrail](https://aws.amazon.com/cloudtrail/) is a web service that records AWS API calls for your account and delivers audit logs to you as JSON files in a S3 bucket. If you do not have it configured, see their documentations on how to do so.

// COMMAND ----------

// MAGIC %md ### Step 1: Where is your input data and where do you want your final Parquet table?
// MAGIC To run this notebook, first of all, you need to specify the location of the CloudTrail logs files. You can open your CloudTrail configuration, find the bucket and set the value below.

// COMMAND ----------

val cloudTrailLogsPath = "s3://bucket-name-for-customer/AWSLogs/*/CloudTrail/*/2020/*/*/"
val parquetOutputPath = "/tmp/cloudTrailRaw/"  // DBFS or S3 path

// COMMAND ----------

// MAGIC %md Note that this uses globs to read logs across all the accounts and all the AWS regions that are being reported to the bucket. If you want to limit your processing to a smaller subset of the logs, change the above path accordingly.

// COMMAND ----------

// MAGIC %md ### Step 2: What is the schema of your data?
// MAGIC To parse the JSON files, we need to know schema of the JSON data in the log files. Below is the schema defined based on the format defined in [CloudTrail documentation](http://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference.html). It is essentially an array (named Records) of fields related to events, some of which are nested structures.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

val cloudTrailSchema = new StructType()
  .add("Records", ArrayType(new StructType()
    .add("additionalEventData", StringType)
    .add("apiVersion", StringType)
    .add("awsRegion", StringType)
    .add("errorCode", StringType)
    .add("errorMessage", StringType)
    .add("eventID", StringType)
    .add("eventName", StringType)
    .add("eventSource", StringType)
    .add("eventTime", StringType)
    .add("eventType", StringType)
    .add("eventVersion", StringType)
    .add("readOnly", BooleanType)
    .add("recipientAccountId", StringType)
    .add("requestID", StringType)
    .add("requestParameters", MapType(StringType, StringType))
    .add("resources", ArrayType(new StructType()
      .add("ARN", StringType)
      .add("accountId", StringType)
      .add("type", StringType)
    ))
    .add("responseElements", MapType(StringType, StringType))
    .add("sharedEventID", StringType)
    .add("sourceIPAddress", StringType)
    .add("serviceEventDetails", MapType(StringType, StringType))
    .add("userAgent", StringType)
    .add("userIdentity", new StructType()
      .add("accessKeyId", StringType)
      .add("accountId", StringType)
      .add("arn", StringType)
      .add("invokedBy", StringType)
      .add("principalId", StringType)
      .add("sessionContext", new StructType()
        .add("attributes", new StructType()
          .add("creationDate", StringType)
          .add("mfaAuthenticated", StringType)
        )
        .add("sessionIssuer", new StructType()
          .add("accountId", StringType)
          .add("arn", StringType)
          .add("principalId", StringType)
          .add("type", StringType)
          .add("userName", StringType)
        )
      )
      .add("type", StringType)
      .add("userName", StringType)
      .add("webIdFederationData", new StructType()
        .add("federatedProvider", StringType)
        .add("attributes", MapType(StringType, StringType))
      )
    )
    .add("vpcEndpointId", StringType)))

// COMMAND ----------

// MAGIC %md ### Step 3: Let's do streaming ETL on it!
// MAGIC Now, we can start reading the data and writing to Parquet table. First, we are going to create the streaming DataFrame that represents the raw records in the files, using the schema we have defined.
// MAGIC We are also option `maxFilesPerTrigger` to get earlier access the final Parquet data, as this limit the number of log files processed and written out every trigger.

// COMMAND ----------

val rawRecords = spark.readStream
  .option("maxFilesPerTrigger", "100")
  .schema(cloudTrailSchema)
  .json(cloudTrailLogsPath)

// COMMAND ----------

// MAGIC %md Then, we are going to transform the data in the following way.
// MAGIC
// MAGIC 1. `Explode` (split) the array of records loaded from each file into separate records.
// MAGIC 2. Parse the string event time string in each record to Spark’s timestamp type.
// MAGIC 3. Flatten out the nested columns for easier querying.

// COMMAND ----------

val cloudTrailEvents = rawRecords
  .select(explode($"Records") as "record")
  .select(
    unix_timestamp($"record.eventTime", "yyyy-MM-dd'T'hh:mm:ss").cast("timestamp") as "timestamp",
    $"record.*")

// COMMAND ----------

// MAGIC %md Finally, we can define how to write out the transformed data and start the `StreamingQuery`. We are going to do the following
// MAGIC
// MAGIC - Write the data out in the Parquet format,
// MAGIC - Define the `date` column from that `timestamp` and partition the Parquet data by date for efficient time-slice queries.
// MAGIC - Define the trigger to be every 10 seconds.
// MAGIC - Define the checkpoint location
// MAGIC - Finally, start the query.

// COMMAND ----------

val checkpointPath = "/cloudtrail_ck.checkpoint/"

val streamingETLQuery = cloudTrailEvents
  .withColumn("date", $"timestamp".cast("date"))
  .writeStream
  .format("parquet")
  .option("path", parquetOutputPath)
  .partitionBy("date")
  .trigger(ProcessingTime("10 seconds"))
  .option("checkpointLocation", checkpointPath)
  .start()

// COMMAND ----------

// MAGIC %md The `streamingETLQuery` should be running in the background. See the `Details` to understand the progress.

// COMMAND ----------

// MAGIC %md ### Step 4: Query up-to-the-minute data from Parquet Table
// MAGIC
// MAGIC While the `streamingETLQuery` is continuously converting the data to Parquet, you can already start running ad-hoc queries on the Parquet table.
// MAGIC Let's define a table/view in Spark on the Parquet files.
// MAGIC If you see an error, then probably the table has not been created yet; wait for a few minutes for the streaming query to write some data and create the table.

// COMMAND ----------

// MAGIC %md If you count the number of rows in the table, you should find the value increasing over time. Run the following every few minutes.

// COMMAND ----------

sql(s"select * from parquet.`$parquetOutputPath`").count()

// COMMAND ----------

display(sql(s"select * from parquet.`$parquetOutputPath`"))
