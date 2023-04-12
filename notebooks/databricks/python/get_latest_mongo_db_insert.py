# Databricks notebook source
dbutils.widgets.text('date', '')
dbutils.widgets.text('collection', '')
dbutils.widgets.text('secrets_info', '')
dbutils.widgets.text('source_time_col', '')
dbutils.widgets.text('env', '')

date = dbutils.widgets.get('date')
collection = dbutils.widgets.get('collection')
secrets_info = dbutils.widgets.get('secrets_info')
source_time_col = dbutils.widgets.get('source_time_col')
env = dbutils.widgets.get('env')

print("date:", date)
print("env:", env)
print("collection:", collection)
print("secrets_info:", secrets_info)
print("source_time_col:", source_time_col)


# COMMAND ----------

# Standard Library
from datetime import datetime

from kms_conn_manager.kms_conn_manager import get_connection_credentials_by_key
from kms_conn_manager.kms_conn_manager import get_text_by_key
from kms_conn_manager.kms_conn_manager import create_or_update_secret

# COMMAND ----------

credentials_info = "airflow/shared/connections/mongo/{}/{}".format(
  secrets_info
  , env
)

credentials = get_connection_credentials_by_key(credentials_info)

user = credentials["user"]
password = credentials["password"]
host = credentials["host"]
port = credentials["port"]
auth_db = credentials["auth_db"]

# COMMAND ----------

source_df = (
  spark.read
  .format("com.mongodb.spark.sql.DefaultSource")
  .option("uri", "mongodb://{}:{}@{}:{}/{}".format(user, password, host, port, auth_db))
  .option("database", "{}".format(auth_db))
  .option("collection", "{}".format(collection))
  .option("readSelection.name", "secondary")
  .load()
)

# COMMAND ----------

run_bool = False
source_max_date = source_df.selectExpr("max(paidAt)").first()[0].date()
sensor_date = datetime.strptime(date, '%Y-%m-%d').date()

if not source_max_date: 
    #just in case there's some sort of connectivity error or the db is empty
    dbutils.notebook.exit(json.dumps({'status': 'failed', 'reason': 'missing data'}))
elif source_max_date > sensor_date: 
    #if the max date in the source is greater than the date we are passing in (mimics the interval=1day in the the timeseries sensor)
    run_bool = True
    dbutils.notebook.exit(str(run_bool))
else:
    dbutils.notebook.exit(json.dumps({'status': 'retrying', 'reason': 'data not caught up'}))

