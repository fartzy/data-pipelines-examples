# Databricks notebook source
dbutils.library.install(
    "s3://central-data-warehouse-prod/libs/python/dw_spark/latest.egg"
)
dbutils.library.installPyPI("boto3", version="1.9.252")
dbutils.library.installPyPI("pyyaml")

import json, re

from pyspark.sql.types import *
from functools import reduce
from pyspark.sql.functions import udf, lit, col
from datetime import date

from dw_spark.common.persistence import write_to_parquet
from dw_spark.common.raw_table_util import get_nonempty_versions
from dw_spark.common.glue import create_or_update_partition
from dw_spark.common.util import is_table_empty

dbutils.widgets.text("partition_date", "2019-11-06")
partition_date = str(dbutils.widgets.get("partition_date"))

schema_name = "dataset_raw"
table_name = "accounts_audit_event"
target_path = "s3://data-warehouse-prod/ins/raw/{table}".format(
    table=table_name
)
partition_path = "{target_path}/partition_date={partition_date}".format(**locals())

partition_num = 10
log = []

print("Table {}.{} ({})".format(schema_name, table_name, partition_date))

# COMMAND ----------

# Excluded m1 due to incompatible schemas; revisit?
unify_target_df = table("ins.accountauditeventm6")
m6df = unify_target_df.withColumn("raw_file_data", lit("m6"))

# COMMAND ----------


@udf((unify_target_df.schema["governmentIdCreateDetails"]).dataType)
def govIdm4():
    return None


@udf((unify_target_df.schema["customerAllocationDetails"]).dataType)
def customerAllocationm4(rawAccountCreate):
    if rawAccountCreate is None:
        return None
    account_create_raw = rawAccountCreate.asDict()
    account_create_dict = {
        "state": account_create_raw["state"],
        "accountId": account_create_raw["accountId"],
        "region": account_create_raw["region"],
        "username": account_create_raw["username"],
        "country": account_create_raw["country"],
        "campaign": None,
        "source": None,
    }
    return account_create_dict


m4_df = (
    table("ins.accountauditeventm4")
    .withColumn("raw_file_data", lit("m4"))
    .withColumn("geoIdentitySuccessDetails", lit(None))
    .withColumn("governmentIdCreateDetails", govIdm4())
    .withColumn(
        "customerAllocationDetails", customerAllocationm4(col("customerAllocationDetails"))
    )
    .select([col(c) for c in m6df.columns])
)

# COMMAND ----------

m5_df = (
    table("ins.accountauditeventm5")
    .withColumn("raw_file_data", lit("m5"))
    .withColumn("geoIdentitySuccessDetails", lit(None))
    .select([col(c) for c in m6df.columns])
)

# COMMAND ----------

unified_records = reduce(lambda df1, df2: df1.union(df2), [m6df, m5_df, m4_df])

# COMMAND ----------

# Persist
source_df = (
    unified_records.distinct().where(col("partition_date") == partition_date).cache()
)

missing_versions = get_nonempty_versions(
    sqlContext, "ins", "accountauditevent", partition_date=partition_date, min_version=6
)

if missing_versions:
    raise Exception(
        "Notebook does not include the following versions of the table {}: {}".format(
            "ins.accountauditevent", missing_versions
        )
    )
elif is_table_empty(sqlContext, df=source_df):
    create_or_update_partition(
        schema_name=schema_name,
        table_name=table_name,
        partition_values=[partition_date],
        partition_location=partition_path,
    )

    log.append("Wrote empty partition for {}".format(partition_date))
else:
    log += write_to_parquet(
        source_df, schema_name, table_name, "partition_date", partition_date
    )

source_df.unpersist()
dbutils.notebook.exit(json.dumps(log))
