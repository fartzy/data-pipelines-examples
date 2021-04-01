-- Databricks notebook source

--enbales dbio cache which is good to have and not on by default unless it is ix3 type ec2 node
set spark.databricks.io.cache.enabled = "true"
set spark.databricks.io.cache.compression.enabled = "false"

--speed up the way delta checks for metadata
set spark.databricks.delta.checkpointV2.enabled = "false"
set spark.databricks.delta.fastQueryPath.enabled = "false"

--speeds up the way delta handles the bin packed files
set spark.databricks.delta.fastQueryPath.dataskipping.enabled = "false"

--specifies the size of input partitions
set spark.sql.files.maxPartitionBytes = 1024 * 1024 * 32 

--default of 200 is too low
set spark.sql.shuffle.partitions = 2560

--for adaptive query execution (AQE) new in spark 3.0 (DBR 7.0)
--by default I set it to true usually 
set spark.sql.adaptive.enabled = "false"

set spark.sql.adaptive.skewJoin.enabled = "true"
set spark.sql.adaptive.coalescePartitions.enabled = "true"
set spark.sql.adaptive.coalescePartitions.minPartitionNum = 640
set spark.databricks.adaptive.autoBroadcastJoinThreshold = "50m"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo_database

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo_database.encounter_provider (
  ENCID STRING COMMENT 'Encounter ID; Links to ENCOUNTER table.',
  PROVID STRING COMMENT 'Identifies physician associated with the encounter; links to PROVIDER table',
  PROVIDER_ROLE	STRING COMMENT 'Describes the role of the physician within the context of the encounter (See list =>)',
  SOURCEID STRING COMMENT 'Identifies the provider group from which the clinical record was generated for the patient. '
)
USING DELTA
LOCATION 's3://bucket-name-1/data/demo_database.db/encounter_provider'
COMMENT 'Encounter_Provider Bridge table'

-- COMMAND ----------
--to_date(INCOMING_STRING_DATE, 'MM-dd-yyyy') as OUT_DATE,
COPY INTO demo_database.encounter_provider
FROM (
  SELECT 
    ENCID,
    PROVID,
    PROVIDER_ROLE,
    SOURCEID
  FROM "s3a://legacy/path/folder"
  )
FILEFORMAT = CSV 
PATTERN = "*_enc_prov_*.txt.gz"
FORMAT_OPTIONS('header'='true', 
               'sep'='|', 
               'inferSchema' = 'false', 
               'mergeSchema' = 'false',
               'io.compression.codecs' = 'nl.basjes.hadoop.io.compress.SplittableGzipCodec')
COPY_OPTIONS ('force'='true')

-- COMMAND ----------

describe detail demo_database.encounter_provider

-- COMMAND ----------

select PROVIDER_ROLE
     , count(*) as provider_count
from demo_database.encounter_provider 
group by PROVIDER_ROLE 
order by count(*) desc
