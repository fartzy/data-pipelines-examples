# Databricks notebook source
env = dbutils.widgets.get("env")
print("env: {env}".format(env=env))

# COMMAND ----------

schema = "test_migration"  # TODO: change to sandbox
table = "buy_store_package_item"

# COMMAND ----------

# DLL

spark.sql("""
  create table if not exists `{schema}`.`{table}` (
        bundle_id string
      , item_id string
      , bundle_release_date date
      , item_release_date date
      , discount float
      , insertion_datetime timestamp 
  )
  partitioned by (
      env string
  )
  row format serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
  with serdeproperties (
    'serialization.format' = '1'
  )
  stored as
  inputformat 'org.apache.hadoop.mapred.TextInputFormat'
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  tblproperties (
    'owners' = 'mikea',
    'populatedby' = 'Databricks',
    'notebookpath' = '/data/path/legacy-migration/{schema}/{table}'
  )
""".format(schema=schema, table=table))

# COMMAND ----------

df = spark.sql("""
  with sib_partial as (
    select env
         , f['id'] as bundle_id
         , reverse(substr(reverse(substr(regexp_replace(get_json_object(f['metadata'], '$.bundledItems'),'/s',''),3)),3)) as bundle_split
         , f['metadata'] as metadata
    from demo_database.buy_item
    where f['id'] like 'bundle%' 
      and get_json_object(f['metadata'], '$.flexibleBundle') = 'true'
      and env = '{env}' 
  ), 
  sib as (
    select env
         , bundle_id
         , get_json_object(concat('{{',b.blob,'}}'), '$.discount') as discount
         , get_json_object(concat('{{',b.blob,'}}'), '$.inventoryType') as inventory_type
         , get_json_object(concat('{{',b.blob,'}}'), '$.itemId') as item_id_num
    from sib_partial lateral view explode(split(bundle_split,',')) b as blob    
  ),
  sii as (
    select env
         , f['id'] as item_id
         , f['inventory_type'] as inventory_type
         , f['item_id'] as item_id_num
         , f['rp_cost'] as rp_price
    from lol_raw.store_item
    where env = 'NA1'
  ),
  siiNA as (
    select env
         , f['id'] as item_id_NA
         , f['inventory_type'] as inventory_type
         , f['item_id'] as item_id_num
         , f['rp_cost'] as rp_price
    from lol_raw.store_item
    where env = 'NA1'
  ),
  old_bundles as (
    select env
         , f['bundle_item_id'] as bundle_id
         , f['item_id'] as item_id
         , case 
             when f['discount_rp_cost'] = 0 then 1.0
             else round(1-(f['discount_rp_cost']/100.0),2) 
           end as discount 
         , null as metadata 
    from lol_raw.store_bundle_item
    where env = 'NA1'
  ),
  sb as (
    select sib.env
         , bundle_id
         , coalesce(item_id, item_id_NA, bundle_id) as item_id
         , discount
    from sib left outer join sii 
      on sii.env = sib.env
      and sii.inventory_type = sib.inventory_type
      and sii.item_id_num = sib.item_id_num
    left outer join siiNA
      on siiNA.inventory_type = sib.inventory_type
      and siiNA.item_id_num = sib.item_id_num

    union all
  
    select env 
         , bundle_id
         , item_id
         , discount
    from old_bundles    
  ), 
  sirb as (
    select env
         , item_id as bundle_id
         , release_date as bundle_release_date
    from lol_derived.store_item_releases
    where env = '{env}'
  ),
  sirbNA as (
    select env
         , item_id as bundle_id
         , release_date as bundle_release_date_NA
    from lol_derived.store_item_releases
    where env = 'NA1'  
  ), 
  siri as (
    select env 
         , item_id
         , release_date as item_release_date
    from lol_derived.store_item_releases
    where env = '{env}'
  ), 
  siriNA as (
    select env
         , item_id
         , release_date as item_release_date_NA
    from lol_derived.store_item_releases
    where env = 'NA1'  
  )
  
  select sb.bundle_id
       , sb.item_id
       , coalesce(bundle_release_date, bundle_release_date_NA) as bundle_release_date
       , coalesce(item_release_date, item_release_date_NA) as item_release_date
       , coalesce(discount,0) as discount
       , current_timestamp() as insertion_datetime
       , sb.env
  from sb left outer join sirb
    on sirb.bundle_id = sb.bundle_id
    and sirb.env = sb.env
  left outer join sirbNA
    on sirbNA.bundle_id = sb.bundle_id
  left outer join siri 
    on siri.item_id = sb.item_id
    and siri.env = sb.env
  left outer join siriNA
    on siriNA.item_id = sb.item_id
""".format(env=env))

# COMMAND ----------

temp_table = "tmp_{}".format(table)
df.createOrReplaceTempView(temp_table)
spark.sql("""
    insert overwrite table `{schema}`.`{table}`
    partition (`env`='{env}')
    select bundle_id 
         , item_id 
         , bundle_release_date 
         , item_release_date 
         , discount 
         , insertion_datetime  
    from {temp_table}
""".format(schema=schema, table=table, 
           temp_table=temp_table, env=env))
