# Databricks notebook source
dbutils.widgets.text("partition_date", "2018-10-26")

# COMMAND ----------

dbutils.library.installPyPI("boto3", "1.9.231")
dbutils.library.installPyPI("pyyaml")
dbutils.library.install(
    "s3://data-warehouse-prod/libs/python/dw_spark/latest.egg"
)
dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime, timedelta
import json
import string

from pyspark.sql.functions import lit

from dw_spark.common.glue import create_or_update_partition
from dw_spark.common.persistence import write_to_parquet

# COMMAND ----------

partition_date = dbutils.widgets.get("partition_date")
previous_date = (datetime.strptime(partition_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

tasks_per_cluster = 1024
spark.conf.set(
    "spark.sql.shuffle.partitions", tasks_per_cluster
)

log = []

schema_name = 'acme_dw'
daily_table_name = 'actor_agreement_instantiation_status_daily_snapshot'
latest_view_name = 'actor_agreement_instantiation_status'

# COMMAND ----------

milestone_info = {
  '2019-11-27': {
    'milestone_name': 'm5',
    'default_agreement':
    {
      'story': {
        'id': 'ffee7721-fe01-4b8a-8de9-ffee77214433',
        'name': 'M4 Story agreement'
      },
      'special': {
        'id': 'ffee7721-9240-4bf3-a9f5-ffee77214433',
        'name': 'Carnivore'
      }
    }
  },
  '2020-02-08': {
    'milestone_name': 'f&f',
    'default_agreement':
    {
      'story': {
        'id': None,
        'name': None
      },
      'special': {
        'id': 'ffee7721-4b3d-46de-a6d7-ffee77214433',
        'name': 'NPE Special agreement'
      }
    }
  }
}

milestone_dates = sorted([*milestone_info])
pointer = 0
while pointer < (len(milestone_dates)):
  if partition_date >= milestone_dates[pointer]:
    pointer += 1
  else:
    break

milestone_date = milestone_dates[pointer-1]

default_story_agreement_id = milestone_info[milestone_date]['default_agreement']['story']['id']
default_story_agreement_name = milestone_info[milestone_date]['default_agreement']['story']['name']
default_special_agreement_id = milestone_info[milestone_date]['default_agreement']['special']['id']
default_special_agreement_name = milestone_info[milestone_date]['default_agreement']['special']['name']

# this should force the below query to not pull in previous milestone's data
if partition_date == milestone_date:
  previous_date_union_subquery = ""
else:
  previous_date_union_subquery = """
    union
    select puuid
         , agreement_id
         , agreement_name
         , agreement_type
         , last_instantiation_timestamp_utc as timestamp_utc
         , agreement_status
         , shard
    from {schema_name}.{daily_table_name}
    where partition_date = '{previous_date}'
  """.format(**locals())

if not default_story_agreement_id:
  default_story_agreement_subquery = ""
  default_story_agreement_union = ""
else:
  default_story_agreement_subquery = """
  story_agreement_default as (
    select distinct(puuid) as puuid
         , '{default_story_agreement_id}' as agreement_id
         , '{default_story_agreement_name}' as agreement_name
         , 'Story' as agreement_type
         , '{partition_date}T00:00:00.000000000Z' as last_instantiation_timestamp_utc
         , 'Default' as agreement_status
         , shard
    from today_and_previous_latest_agreement_instantiation_events
  ),

  anti_join_story as (
    select story.puuid as puuid
         , story.agreement_id as agreement_id
         , story.agreement_name as agreement_name
         , story.agreement_type as agreement_type
         , story.last_instantiation_timestamp_utc as last_instantiation_timestamp_utc
         , story.agreement_status as agreement_status
         , story.shard as shard
    from story_agreement_default story
    anti join today_and_previous_latest_agreement_instantiation_events cae
           on story.puuid = cae.puuid
          and story.agreement_id = cae.agreement_id
  ),
  """.format(**locals())
  default_story_agreement_union = (
    """union
  select * from anti_join_story
    """
  )

if not default_special_agreement_id:
  default_special_agreement_subquery = ""
  default_special_agreement_union = ""
else:
  default_special_agreement_subquery = """
  special_agreement_default as (
    select distinct(puuid) as puuid
         , '{default_special_agreement_id}' as agreement_id
         , '{default_special_agreement_name}' as agreement_name
         , 'Special' as agreement_type
         , '{partition_date}T00:00:00.000000000Z' as last_instantiation_timestamp_utc
         , 'Default' as agreement_status
         , shard
    from today_and_previous_latest_agreement_instantiation_events
  ),

  anti_join_special as (
    select special.puuid as puuid
         , special.agreement_id as agreement_id
         , special.agreement_name as agreement_name
         , special.agreement_type as agreement_type
         , special.last_instantiation_timestamp_utc as last_instantiation_timestamp_utc
         , special.agreement_status as agreement_status
         , special.shard as shard
    from special_agreement_default special
    anti join today_and_previous_latest_agreement_instantiation_events cae
           on special.puuid = cae.puuid
          and special.agreement_id = cae.agreement_id
  ),
  """.format(**locals())
  default_special_agreement_union = (
    """union
  select * from anti_join_special
    """
  )

# COMMAND ----------

# take all of today's instantiation events, union with latest instantiation status
# find latest instantiation event per agreement type
# union with default agreement values (if they never switch active agreements, they will never show up)
# figure out what the latest instantiation event is with defaults added in

actor_agreement_instantiation_status_daily_df = sql("""
with today_and_previous_agreement_instantiation_events as (
  select puuid
       , initiated_agreement_id as agreement_id
       , initiated_agreement_name as agreement_name
       , initiated_agreement_type as agreement_type
       , timestamp_utc
       , null as agreement_status
       , shard
  from acme_clean.agreement_instantiation_event
  where partition_date = '{partition_date}'
  {previous_date_union_subquery}
),

today_and_previous_latest_agreement_instantiation_events_wout_status as (
  select puuid
       , agreement_id
       , max(timestamp_utc) as timestamp_utc
  from today_and_previous_agreement_instantiation_events
  group by puuid, agreement_id
),

today_and_previous_latest_agreement_instantiation_events as (
  select events.puuid as puuid
       , events.agreement_id as agreement_id
       , events.agreement_name as agreement_name
       , events.agreement_type as agreement_type
       , events.timestamp_utc as timestamp_utc
       , events.agreement_status as agreement_status
       , events.shard as shard
  from today_and_previous_latest_agreement_instantiation_events_wout_status latest
  left join today_and_previous_agreement_instantiation_events events
         on latest.puuid = events.puuid
        and latest.agreement_id = events.agreement_id
        and latest.timestamp_utc = events.timestamp_utc
),

{default_story_agreement_subquery}

{default_special_agreement_subquery}

cae_with_default as (
  select puuid
       , agreement_id
       , agreement_name
       , agreement_type
       , timestamp_utc as last_instantiation_timestamp_utc
       , agreement_status
       , shard
  from today_and_previous_latest_agreement_instantiation_events
  {default_story_agreement_union}
  {default_special_agreement_union} 
),


cae_last_initiated_agreement_type as (
  select puuid
       , agreement_type
       , max(last_instantiation_timestamp_utc) as last_instantiation_timestamp_utc
  from cae_with_default
  group by puuid, agreement_type
)

select cwd.puuid as puuid
     , cwd.agreement_id as agreement_id
     , cwd.agreement_name as agreement_name
     , cwd.agreement_type as agreement_type
     , cwd.last_instantiation_timestamp_utc as last_instantiation_timestamp_utc
     , case
           when clact.last_instantiation_timestamp_utc is null then 'Inactive'
           else 'Active' 
       end as agreement_status
     , cwd.shard as shard
from cae_with_default cwd
left join cae_last_initiated_agreement_type clact
on cwd.puuid = clact.puuid
and cwd.agreement_type = clact.agreement_type
and cwd.last_instantiation_timestamp_utc = clact.last_instantiation_timestamp_utc
""".format(**locals())).withColumn("partition_date", lit(partition_date)).cache()

# COMMAND ----------

log += write_to_parquet(
    actor_agreement_instantiation_status_daily_df,
    schema_name,
    daily_table_name,
    "partition_date",
    partition_date
)

sql("""
alter table {schema_name}.{daily_table_name}
add if not exists partition (partition_date='{partition_date}')
""".format(**locals()))

actor_agreement_instantiation_status_daily_df.unpersist()

# COMMAND ----------

sql("""
  create or replace view {schema_name}.{latest_view_name} as (
    select *
    from {schema_name}.{daily_table_name}
    where partition_date = '{partition_date}'
)
""".format(**locals()))

# COMMAND ----------

dbutils.notebook.exit(json.dumps(log))
