# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql import functions as f
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

from kms_conn_manager.kms_conn_manager import get_connection_credentials_by_key
from kms_conn_manager.kms_conn_manager import get_text_by_key
from kms_conn_manager.kms_conn_manager import create_or_update_secret
from acme_ds.spark.data_processing.mysql import *
import acme_ds.spark.data_processing.dp_utils as Utils

# COMMAND ----------

# DBTITLE 1,Extract Params
# Passed in Variables
dt = dbutils.widgets.get("dt")
env = dbutils.widgets.get("env")
region_id = Utils.get_env_info(spark, env, "region_id")
run_date = dbutils.widgets.get("dt")

date_col_name = 'w_dt'

production = True

target_database = "airflow"
target_table = "target_table"

# COMMAND ----------

# DBTITLE 1,Production Flag
# override for Testing
if type(production) != bool:
  raise Exception("boolean type expected for 'production'")
if not production:
  arget_database = "migration_test"

# COMMAND ----------

# DBTITLE 1,Read From Hive
target_df = spark.sql("""
  select f['originalAccountId'] as account_id
    , f['dimension1Id'] as dimension1_id
    , f['dimension2Id'] as dimension2_id
    , f['dimension2Mode'] as dimension2_mode
    , f['dimension2Type'] as dimension2_type
    , from_unixtime(cast(f['dimension2CreationTime']/1000 as bigint)) as create_datetime 
    , f['leaver'] as leaver
    , left(f['role'], 10) as role
    , f['category4'] as category4
    , f['completed'] as completed
    , f['timePlayed'] as time_played
    , f['category2Id'] as category2_id
    , f['category1Index'] as category1_index
    , case 
         when f['teamId'] > 127 then 127 
         else f['teamId']
      end as team_id
    , f['dim3Level'] as dim3_level
    , f['wasend'] as was_end
    , f['win'] as win, f['kills'] as kills
    , f['totals'] as totals
    , f['assists'] as assists
    , f['category3'] as category_3
    , f['energySpent'] as energy_spent
    , f['totalTimeSpentDead'] as total_spent_dead
    , f['totalMetricsCategory2s'] as total_damage_category2s
    , dt as w_dt 
    , '{region_id}' as region_id
  from upstream_db.datastore_details_level 
  where dt = '{dt}' 
    and f['originalCategoryId'] = 'LS' 
    and f['timePlayed'] >= 1000 
    and f['dimension2Type'] = 'MATCHED_dimension2' 
    and (f['totals']/round(f['timePlayed']/60) >= 1 or
          f['totalMetricsCategory2s'] = 0)
""".format(dt=dt, region_id=region_id))

# COMMAND ----------

# DBTITLE 1,Check for Source Data
rows_being_written = target_df.cache().count()
assert rows_being_written > 0, "No new data to write (source query returns 0 rows)"

# COMMAND ----------

# DBTITLE 1,MySQL Connection Properties
source_type = "migrate_mysql"

credentials_info = "airflow/shared/connections/mysql/{}/{}".format(
  source_type, 
  env
).lower()

mysql_credentials = get_connection_credentials_by_key(credentials_info)

jdbcUrl = "jdbc:mysql://{host}:{port}/{database}".format(
  host=mysql_credentials["host"], port=mysql_credentials["port"], database=target_database
)

mysql_credentials["port"] = int(mysql_credentials["port"])

connectionProperties = {"user": mysql_credentials["login"], "password": mysql_credentials["password"]}

# COMMAND ----------

# DBTITLE 1,Check for Existence of Data in Target
dt_obj = datetime.strptime(dt, "%Y-%m-%d")
day_range = [dt_obj.strftime("%F %T"), (dt_obj + timedelta(days=1)).strftime("%F %T")]
exists_in_mysql = bool(query_to_list("""
  select *
  from {schema}.{table} 
  where {date_col_name} >= '{start}' 
    and {date_col_name} < '{end}'
    and region_id = {region_id}
  limit 2 
""".format(
  schema=target_database,
  table=target_table,
  start=day_range[0],
  end=day_range[1],
  region_id=region_id,
  date_col_name=date_col_name
      ),
  mysql_credentials,
  )
)

if exists_in_mysql:
  dbutils.notebook.exit("The data exists in MySQL for table {}.{} and date {}".format(
    target_database, target_table, dt 
  )
)

# COMMAND ----------

# DBTITLE 1,For MySQL Limit Connections/Partitions to 10 
print("target table={schema}.{table}".format(schema=target_database, table=target_table))


#  as after group by it creates 200 partitions by default
mysql_partitions = min(target_df.rdd.getNumPartitions(), 10)
mysql_df = target_df.repartition(mysql_partitions)

# COMMAND ----------

safe_append_df_to_table(
  mysql_df, target_database, target_table, mysql_credentials
)

# COMMAND ----------

# DBTITLE 1,Count Inserted Rows
saved_result = query_to_list("""
  select COUNT(*) 
  from {schema}.{table} 
  where {date_col_name} >= '{start}' 
    and {date_col_name} < '{end}'
    and region_id = {region_id}
""".format(
  schema=target_database,
  table=target_table,
  start=day_range[0],
  end=day_range[1],
  region_id=region_id,
  date_col_name=date_col_name
  ),
  mysql_credentials,
)[0][0]

# COMMAND ----------

# DBTITLE 1,Validate Results
assert saved_result == rows_being_written, \
"The data just inserted into MySQL table {}.{} for date '{}' doesn't match the source \
|| source={}  target={}" .format(target_database, target_table, dt, rows_being_written, saved_result)
