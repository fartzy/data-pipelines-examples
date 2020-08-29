# Databricks notebook source
"""
  Imports 
"""
lib1_location = "s3://data-warehouse/libs/python/deprecated/conn_manager/kms_conn_manager-latest.egg"
lib2_location = "s3://data-warehouse/libs/python/spark/latest.egg"

dbutils.library.install(lib2_location)
dbutils.library.install(lib1_location)

from datetime import datetime, timedelta

import psycopg2
from pyspark.sql import Row

from dw_spark.common.persistence import write_to_parquet
from dw_spark import *
from kms_conn_manager.kms_conn_manager import get_conn_creds_by_key

# COMMAND ----------

glu_schema = "dw_db"
glu_table = "stats_details"
psql_schema = "raw"
psql_table = "stats_details"
root_notebook_path = "/Data-Processing"
run_date = dbutils.widgets.get("dt")

# COMMAND ----------


"""
run_date was originally meant to be current date but in order to account for lagging metric
run_date will now be set to current date - 1 day. The run_date will actually be the previous day  
before the current date, which was the incoming run_date parameter (the point of the below transformation). 

i.e. if run_date parameter coming in is 2020-03-20 then 
  run_date_plus_one = '2020-03-20'
  run_date = '2020-03-19'
  run_date_minus_one = '2020-03-18'
  
"""
dt_fmt = "%Y-%m-%d"

_old_run_date_minus_one = datetime.strptime(run_date, dt_fmt) + timedelta(days=-1)
old_run_date_minus_one = datetime.strftime(_old_run_date_minus_one, dt_fmt)

run_date_plus_one_tmp = run_date
run_date = old_run_date_minus_one
run_date_plus_one = run_date_plus_one_tmp

print("run_date_plus_one: " + run_date_plus_one + " || run_date: " + run_date)

# COMMAND ----------

"""
  Registers the function 'geog_ip_ext' if does not already exists - this function is used to extract country name
"""
from pyspark.sql import Row

sc.addFile('s3n://acme-data-warehouse/geogip/latest/GeogIP-City.mmdb')
sc.addFile('s3n://acme-data-warehouse/geogip/latest/GeogIP-ISP.mmdb')
if not (Row(function="geog_ip_ext") in spark.sql("show functions").collect()):
    spark.sql("create temporary function geog_ip_ext as 'com.acme.data.hive.udf.GeogIPExt'")

# COMMAND ----------

"""
  Creates table in glue catalog if does not already exists 
"""

spark.sql("""
  create table if not exists `{glu_schema}`.`{glu_table}` (
      truncated_minute string
    , unique_metrics integer
    , region string
    , country string
    , date string
  )
  using delta
  partitioned by ( date )
  tblproperties (
    'owners' = 'martz',
    'populatedby' = 'Databricks',
    'notebookpath' = '{root_notebook_path}/{glu_schema}/{glu_table}'
    )
""".format(glu_schema=glu_schema, glu_table=glu_table, root_notebook_path=root_notebook_path))

# COMMAND ----------

"""
  Creates temporary table `vertical1_details_table` from raw client session data in s3
"""

source_df = spark.read.format("delta").load("s3://acme-analytics-platform-data/prod/ingest/details_table")
source_df.createOrReplaceTempView("vertical1_details_table")

# COMMAND ----------

"""

  Logic for CTEs below 
  filtered_exploaded : inline_outer function explodes the array of structs for column1 
    (extracts elements and increases row cardinaltiy based on column array)
  exploaded_coalesced : coalesces truncated_minute in case either the row never had any 
    values in column1 array
  count_distinct_subjectid : applies approx_count_distinct over a logical time window of the 
    last 11 minutes to get the distinct subjectids
  select : gets the distinct aggregates and filters again by desired run date
  
"""
send_to_glue_df = spark.sql(
    """
  with filtered_exploaded as (
    select date_format(metadata.timestamp, 'yyyy-MM-dd HH:mm:00') as truncated_minute
        , regionId as region
        , subjectinfo.subjectid
        , geog_ip_ext(subjectInfo.ipAddress,'COUNTRY_NAME') as country
        , inline_outer(column1) as (window, timestamp_window)
    from vertical1_details_table
    where partition_date = '{run_date_plus_one}'
       or partition_date = '{run_date}'
  ), 

  exploaded_coalesced as (
    select cast(
            coalesce(
              date_format(timestamp_window, 'yyyy-MM-dd HH:mm:00')
              , truncated_minute
            ) as timestamp
        ) as truncated_minute
      , region
      , country
      , subjectid
    from filtered_exploaded
  ),

  count_distinct_subjectid as (
    select truncated_minute
      , region
      , country
      , approx_count_distinct(subjectid) over (
          partition by region, country 
          order by truncated_minute range between interval '10' minute preceding and current row
        ) as unique_subjectids
    from exploaded_coalesced
  )

  select distinct 
      date_format(truncated_minute, 'yyyy-MM-dd HH:mm:00') as truncated_minute
    , region
    , country
    , unique_subjectids
    , '{run_date}' as partition_date
  from count_distinct_subjectid
  where date_format(truncated_minute, 'yyyy-MM-dd') = '{run_date}'
  order by truncated_minute, region, country
""".format(
        run_date=run_date, run_date_plus_one=run_date_plus_one,
    )
)

send_to_glue_df.cache()
before_insert_count = send_to_glue_df.count()
print(
    "Count of rows being send to {schema}.{table}: {cnt}".format(
        schema=glu_schema, table=glu_table, cnt=before_insert_count
    )
)

# COMMAND ----------

"""
  Write to the database another partition and add the alter table statement  
"""
log = write_to_parquet(send_to_glue_df, glu_schema, glu_table, "partition_date", run_date)

spark.sql(
    """
    alter table {glu_schema}.{glu_table}
    add if not exists 
    partition (partition_date='{partition_date}')
    """.format(
        glu_schema=glu_schema, glu_table=glu_table, partition_date=run_date
    )
)
print(log)

after_insert_count = (
    spark.read.table("{glu_schema}.{glu_table}".format(glu_schema=glu_schema, glu_table=glu_table))
    .where("partition_date = '{run_date}'".format(run_date=run_date))
    .count()
)

assert (
    before_insert_count == after_insert_count
), "Glue validation not passed. {} rows before, {} rows after insert.".format(
    before_insert_count, after_insert_count
)

# COMMAND ----------

"""
  Overwrite the current 'run_date' partition of the glue table 
  This is in case the `write_to_parquet`
"""

# send_to_glue_df.write.format("delta").mode("overwrite").option(
#     "replaceWhere", "date = '{run_date}'".format(run_date=run_date)
# ).saveAsTable("{glu_schema}.{glu_table}".format(glu_schema=glu_schema,glu_table=glu_table))

# after_insert_count = spark.read.table(
#     "{glu_schema}.{glu_table}".format(glu_schema=glu_schema,glu_table=glu_table)
# ).where("date = '{run_date}'".format(run_date=run_date)).count()

# assert before_insert_count == after_insert_count, \
#     "Glue validation not passed. {} rows before, {} rows after insert.".format(before_insert_count, after_insert_count)

# COMMAND ----------

"""
  Currently the DB data is just pulling from the glue dataframe.  
  Basically a second aggregation of the glue data frame - aggregated to hours from minutes.
"""

send_to_glue_df.createOrReplaceTempView("send_to_glue")

psql_df = spark.sql("""
  select date_trunc('hour', to_timestamp(truncated_minute)) as truncated_hour
    , max(unique_metrics) as max_metric
    , avg(unique_metrics) as avg_metric
    , region
    , country 
  from send_to_glue
  group by date_trunc('hour', to_timestamp(truncated_minute))
    , region
    , country
""")

# COMMAND ----------

"""
  Function to delete the date partition from postgres database before inserting the current run_date data
  This acts as an overwrite without overwriting the whole table
"""

def delete_partition(date, props):
    """ delete partition of postgres table by date """
    conn = None
    rows_del = 0
    try:
        conn = psycopg2.connect(
          user=props["user"], 
          password=props["password"],
          host=props["host"],
          port=props["port"], 
          database=props["database"],
        )
       
        cur = conn.cursor()
        del_query = "delete from {psql_schema}.{psql_table} where date(truncated_hour) = '{date}'".format(
          date=date, psql_schema=psql_schema, psql_table=psql_table
        )
        
        cur.execute(del_query)
        rows_del = cur.rowcount
        conn.commit()
        cur.close()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 
    return rows_del

# COMMAND ----------

#in case the function `get_conn_creds_by_key` is not working, can use the below one as well 
import subprocess as sp
import json 
import ast

secret_id = "airflow/connections/postgres/conn1/"
awscli_res = sp.run(["aws", "secretsmanager", "get-secret-value", "--secret-id", secret_id, "--region", "us-east-1"], stdout=subprocess.PIPE,)
aapdb_creds = ast.literal_eval(json.loads(awscli_res.stdout.decode("utf-8"))['SecretString'])

# COMMAND ----------

"""
  Configuring connection to Postgres DB
"""
secret_id = "airflow/connections/postgres/conn1/"
aapdb_creds = get_connection_credentials_by_key(secret_id)

jdbc_hostname = aapdb_creds["host"]
jdbc_port = aapdb_creds["port"]
jdbc_database = aapdb_creds["dbname"]
jdbc_user = aapdb_creds["username"]
jdbc_pwrd = aapdb_creds["password"]
jdbc_url = "jdbc:postgresql://{0}:{1}/{2}".format(jdbc_hostname, jdbc_port, jdbc_database)

connectionProps = {"host": jdbc_hostname}
connectionProps["port"] = jdbc_port
connectionProps["database"] = jdbc_database
connectionProps["user"] = jdbc_user
connectionProps["password"] = jdbc_pwrd

# COMMAND ----------

"""
  Deleting the partition and then inserting the rows into postgres. Makes the 
  notebook update process idempotent
"""
before_insert_count = psql_df.count()

psql_deleted_rows = delete_partition(run_date, connectionProps)
print(
    "{psql_deleted_rows} rows deleted for date '{run_date}'".format(
        psql_deleted_rows=psql_deleted_rows, run_date=run_date
    )
)

psql_df.write.jdbc(
    jdbc_url,
    "{psql_schema}.{psql_table}".format(psql_schema=psql_schema, psql_table=psql_table),
    "append",
    connectionProps,
)

pushdown_query = "(select * from {psql_schema}.{psql_table} where date(truncated_hour) = '{run_date}') tbla".format(
    psql_schema=psql_schema, psql_table=psql_table, run_date=run_date
)

after_insert_count = spark.read.jdbc(
    url=jdbc_url, table=pushdown_query, properties=connectionProps
).count()

assert (
    before_insert_count == after_insert_count
), "Postgres validation not passed. {} rows before, {} rows after insert.".format(
    before_insert_count, after_insert_count
)
