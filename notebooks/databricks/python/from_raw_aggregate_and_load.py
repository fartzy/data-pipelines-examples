# Databricks notebook source
# DBTITLE 1,easel_clean.icon_data
dbutils.library.installPyPI("arrow")
dbutils.library.install(
    "s3://data-warehouse-prod/libs/python/cdw_spark/latest.egg"
)
dbutils.library.installPyPI("boto3", version="1.9.252")
dbutils.library.installPyPI("pyyaml")
dbutils.library.installPyPI("newrelic-telemetry-sdk")

import json
from datetime import datetime

from pyspark.sql.functions import col

from cdw_spark.easel.transformations import to_isoformat
from cdw_spark.tables.date_partitioned_table_with_empty_partitions import DatePartitionedTableWithEmptyPartitions

dbutils.widgets.text("partition_date", "2019-05-01", "partition date")
partition_date = dbutils.widgets.get("partition_date")

schema_name = "easel_clean"
table_name = "icon_data"
target_path = "s3://data-warehouse-prod/easel/clean/{table}".format(
    table=table_name
)
partition_path = "{target_path}/partition_date={partition_date}".format(**locals())

partition_num = 1

log = []

# COMMAND ----------

clean_cards = sqlContext.sql(
    """
SELECT metadata.timestamp                           AS start_time
     , card.cardId                                  AS card_id
     , card.cardName                                AS icon_name
     , type                                         AS type
     , code                                         AS code
     , cast(rarity as int)                          AS rarity
     , cast(cost as int)                            AS cost
     , faction                                      AS faction
     , subtype                                      AS subtype
     , supertype                                    AS supertype
     , cardSet                                      AS card_set
     , partition_date                               AS partition_date
     , COALESCE(patchlineInfo.InteractDataHash, patchlineInfo.clientInteractDataHash, patchlineInfo.serverInteractDataHash)               
                                                   AS interaction_data_hash
     , COALESCE(patchlineInfo.clientInteractDataHash, "Unknown")     
                                                   AS client_interaction_data_hash
     , COALESCE(patchlineInfo.serverInteractDataHash, "Unknown")                         
                                                   AS server_interaction_data_hash
FROM easel_raw.icon_data
WHERE partition_date = '{}'
     """.format(
        partition_date
    )
).dropDuplicates()

# COMMAND ----------

clean_refined_df = (
    clean_cards.withColumn("start_time", to_isoformat("start_time"))
    .select(
        "start_time",
        "card_id",
        "icon_name",
        "type",
        "code",
        "rarity",
        "cost",
        "faction",
        "subtype",
        "supertype",
        "card_set",
        "partition_date",
        "interaction_data_hash",
        "client_interaction_data_hash",
        "server_interaction_data_hash"
    )
    .distinct()
    .repartition(partition_num)
    .cache()
)

# COMMAND ----------

validations = """
- type: groupCount
  column: icon_name
  sample: 0.01
  threshold: 1
  criticality: Warning

- type: nullOrUnknownCheck
  column: icon_name
  sample: 0.01
  criticality: Warning
"""

# COMMAND ----------

icon_data_table = DatePartitionedTableWithEmptyPartitions(
    spark,
    schema_name,
    table_name,
    validations=validations
)
log.append(icon_data_table.save(clean_refined_df))

# COMMAND ----------

spark.catalog.clearCache()
dbutils.notebook.exit(json.dumps(log))
