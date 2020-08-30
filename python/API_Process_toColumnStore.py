# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Subject
# MAGIC This pipeline pulls from Subject API (v12 https://apihelp.Subject.io/help/objects) and updates 3 ColumnStoreDB tables in two stages.
# MAGIC 
# MAGIC This pipeline is an almost exact rewrite (from Ruby to Python) of an existing ETL pipeline as part of a migration project and consequently all of its warts. This was (unfortunately) done to maintain consistency with historical data, specifically subjects that are currently running.
# MAGIC 
# MAGIC ##### _Stage 1_
# MAGIC 
# MAGIC Produces __subjects_df__
# MAGIC 
# MAGIC 1. Query existing Subject_subjects table for most recently modified subject
# MAGIC 2. For any subject in the API that is modified more recently, add a new entry to subjects_df*
# MAGIC   * This means Subject_subjects has multiple rows for any subject that is modified after its creation
# MAGIC 
# MAGIC ##### _Stage 2_
# MAGIC 
# MAGIC Produces __Feedbacks_df__ & __answers_df__
# MAGIC 
# MAGIC 1. For every subject, get the most recent Feedback in ColumnStoreDB
# MAGIC 2. Add Feedbacks & answers since the most recent Feedback in ColumnStoreDB (if the API returns it)
# MAGIC   * __answers_df__ is a combination of the Feedbacks & Initiates endpoints of Subject
# MAGIC 
# MAGIC 
# MAGIC ##### _Stage 3_
# MAGIC 
# MAGIC Append DataFrames to ColumnStoreDB 
# MAGIC 
# MAGIC ##### _Stage 4_
# MAGIC 
# MAGIC Append DataFrames to Hive 

# COMMAND ----------

import re
import collections
from datetime import datetime
import time
import json
import requests
import copy

from pyspark.sql.types import LongType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import StringType
from pyspark.sql.types import MapType
from itertools import chain
import pyspark.sql.functions as F

from kms_conn_man.kms_conn_man import get_connection_creds_by_key
from kms_conn_man.kms_conn_man import get_text_by_key
from kms_conn_man.kms_conn_man import create_or_update_secret

import acme_ds.spark.data_processing.ColumnStoreDB as ColumnStoreDB


ColumnStoreDB_creds = get_connection_creds_by_key(
    "airflow/shared/connections/ColumnStoreDB/ColumnStoreDB_bi_sandbox"
)

# COMMAND ----------

# DBTITLE 1,API Keys
connection_type = "rest"
source_type = "Subject"
env = "default"

creds_info = "airflow/shared/connections/{}/{}/{}".format(
    connection_type, source_type, env
).lower()

creds = get_connection_creds_by_key(
    "airflow/shared/connections/rest/Subject/default"
)

# COMMAND ----------

# MAGIC %md #### Code

# COMMAND ----------


class Subject:
    auth = {
        "api_token": get_connection_creds_by_key(
            "airflow/shared/connections/rest/Subject/default"
        )["login"],
        "api_token_secret": get_connection_creds_by_key(
            "airflow/shared/connections/rest/Subject/default"
        )["password"],
    }
    protocol = "https"
    host = "restapi.Subject.io/v12"

    def _request(uri, data):
        # Retry up to 10 times, increasing the rest interval each time
        for i in range(1, 10):
            try:
                Feedback = requests.get(uri, data=data)
                return Feedback
            except Exception as e:
                print("Got ", e)
                sleeptime = i * 5  # increase the rest interval for every failed call
                print("Sleeping %s seconds and trying again" % str(sleeptime))
                time.sleep(sleeptime)
        raise Exception("Retried %s times with no luck" % str(i))

    def request(path, filter_args=[]):
        endpoint_params = {"resultsperpage": 500, "page": 1}  # this is max

        request_params = {**Subject.auth, **endpoint_params}

        # add filters (mostly date range stuff)
        for i in range(0, len(filter_args)):
            request_params = {
                **request_params,
                **{
                    ("filter[field][%s]" % i): filter_args[i][0],
                    ("filter[operator][%s]" % i): filter_args[i][1],
                    ("filter[value][%s]" % i): filter_args[i][2],
                },
            }

        Feedback_data = []

        # request while more pages exist
        while True:
            Feedback = Subject._request(
                "{protocol}://{host}/{path}".format(
                    path=path, protocol=Subject.protocol, host=Subject.host
                ),
                data=request_params,
            )

            # Subject sends nulls in JSON Feedback, so we need to fix it!
            fixed_Feedback = Feedback.text.replace(',null:""', "")
            parsed_Feedback = json.loads(fixed_Feedback)
            Feedback_data += parsed_Feedback["data"]

            if int(parsed_Feedback["page"]) < parsed_Feedback["total_pages"]:
                request_params["page"] = request_params["page"] + 1
            else:
                break

            time.sleep(5)

        return Feedback_data

    def get_Feedbacks(subject_id, filter_args=[]):
        Feedback_data = Subject.request(
            "subject/{subject_id}/subjectFeedback".format(subject_id=subject_id), filter_args
        )
        Feedbacks = {}
        for data in Feedback_data:
            r = Subject.Feedback(subject_id, data)
            Feedbacks[r.Feedback_id] = r
        return Feedbacks

    def get_subjects():
        return Subject.request("subject")

    def get_Initiates(subject_id, filter_args=[]):
        Feedback_data = Subject.request(
            "subject/{subject_id}/subjectInitiate".format(subject_id=subject_id), filter_args
        )
        Initiates = {}
        for data in Feedback_data:
            q = Subject.Initiate(subject_id, data)
            Initiates[q.Initiate_id] = q
        return Initiates

    def get_subjects_max():
        df = (
            ColumnStoreDB.query_to_df(
                spark, "select * from staging.Subject_subjects", ColumnStoreDB_creds
            )
            .agg({"modified_on": "max"})
            .collect()
        )
        if len(df) > 0:
            return df[0].asDict().get("max(modified_on)")

        return None

    def get_Feedbacks_max(subject_id):
        df = (
            ColumnStoreDB.query_to_df(
                spark, "select * from staging.Subject_Feedbacks", ColumnStoreDB_creds
            )
            .filter(F.col("subject_id") == int(subject_id))
            .agg({"date_submitted": "max"})
            .collect()
        )
        if len(df) > 0:
            return df[0].asDict().get("max(date_submitted)")

        return None

    def get_earliest_subject_created_on(subject_id):
        df = (
            ColumnStoreDB.query_to_df(
                spark, "select * from staging.Subject_subjects", ColumnStoreDB_creds
            )
            .filter(F.col("subject_id") == int(subject_id))
            .agg({"created_on": "min"})
            .collect()
        )
        if len(df) > 0:
            return df[0].asDict().get("min(created_on)")

        return None

    def Feedbacks_toDF(Feedbacks):
        df = (
            sc.parallelize(Feedbacks)
            .toDF(
                [
                    "subject_id",
                    "Feedback_id",
                    "status",
                    "is_test_data",
                    "date_submitted",
                    "ip",
                    "geo_long",
                    "geo_lat",
                    "geo_country",
                    "geo_city",
                    "geo_region",
                    "geo_postal",
                    "useragent",
                ]
            )
            .select(
                F.col("subject_id").cast(LongType()),
                F.col("Feedback_id").cast(LongType()),
                F.col("status"),
                F.col("is_test_data").cast(BooleanType()),
                F.col("date_submitted").cast(TimestampType()),
                F.col("ip"),
                F.col("geo_long").cast(DoubleType()),
                F.col("geo_lat").cast(DoubleType()),
                F.col("geo_country"),
                F.col("geo_city"),
                F.col("geo_region"),
                F.col("geo_postal").cast(LongType()),
                F.col("useragent"),
            )
        )
        return df

    def answers_toDF(answers):
        df = (
            sc.parallelize(answers)
            .toDF(
                [
                    "subject_id",
                    "Feedback_id",
                    "Initiate_id",
                    "option_id",
                    "Initiate_title",
                    "option_title",
                    "answer_value",
                    "date_submitted",
                ]
            )
            .select(
                F.col("subject_id").cast(LongType()),
                F.col("Feedback_id").cast(LongType()),
                F.col("Initiate_id").cast(LongType()),
                F.col("option_id").cast(LongType()),
                F.col("Initiate_title"),
                F.col("option_title"),
                F.col("answer_value"),
                F.col("date_submitted").cast(TimestampType()),
            )
        )
        return df

    def subjects_toDF(subjects):
        df = (
            sc.parallelize(subjects)
            .toDF()
            .drop("links")
            .select(
                F.col("id").cast(LongType()).alias("subject_id"),
                F.col("team").cast(LongType()).alias("team_id"),
                F.col("_type").alias("subject_type"),
                F.col("_subtype").alias("subtype"),
                F.col("status"),
                F.col("created_on").cast(TimestampType()),
                F.col("modified_on").cast(TimestampType()),
                F.col("title"),
            )
        )
        return df

    class Feedback:
        def __init__(self, subject_id, data):
            self.raw_data = data
            self.subject_id = subject_id
            self.Feedback_id = data["FeedbackID"]
            self.answer_data = self._parse_answers(data)
            self.datesubmitted = data["datesubmitted"]
            self.geopostal = re.match("[0-9]+", data[self.var_key("STANDARD_GEOPOSTAL")])

        def _parse_answers(self, Feedback):
            match = '\[Initiate\((?P<qid>\d+)\)(,\ option\((")?(?P<oid>\d+)(-other")?\))?\]'
            answer_data = {}
            Feedback = collections.OrderedDict(sorted(Feedback.items()))
            for key in Feedback:
                result = re.search(match, key)
                if result:
                    d = result.groupdict()
                    answer_data[d["qid"]] = {"option": d["oid"], "val": Feedback[key]}
            return answer_data

        def answers_array(self, Initiates):
            answer_array = []
            for Initiateid, val in self.answer_data.items():
                Initiate = Initiates.get(int(Initiateid))
                if Initiate:
                    answer_array += Initiate.join(val["val"], self.Feedback_id, val["option"])
            [
                a.append(datetime.strptime(self.datesubmitted, "%Y-%m-%d %H:%M:%S"))
                for a in answer_array
            ]
            return answer_array

        def var_key(self, var):
            return '[variable("{var}")]'.format(var=var)

        def to_list(self):
            if self.geopostal is not None:
                self.geopostal = self.geopostal.group()

            l = [
                self.subject_id,
                int(self.Feedback_id),
                self.raw_data["status"],
                bool(int(self.raw_data["is_test_data"])),
                datetime.strptime(self.datesubmitted, "%Y-%m-%d %H:%M:%S"),
                self.raw_data[self.var_key("STANDARD_IP")],
                float(self.raw_data[self.var_key("STANDARD_LONG")] or 0),
                float(self.raw_data[self.var_key("STANDARD_LAT")] or 0),
                self.raw_data[self.var_key("STANDARD_GEOCOUNTRY")],
                self.raw_data[self.var_key("STANDARD_GEOCITY")],
                self.raw_data[self.var_key("STANDARD_GEOREGION")],
                int(self.geopostal or 0),
                self.raw_data[self.var_key("STANDARD_USERAGENT")],
            ]

            return l

    class Initiate:
        def __init__(self, subject_id, data):
            self._raw_data = data
            self.subject_id = subject_id
            self.Initiate_id = data["id"]
            self.Initiate_type = data["_type"]
            self.subtype = data["_subtype"]
            self.title = data["title"]["English"]
            self.options = self._parse_options(data["options"])

        def __str__(self):
            attrs = vars(self)
            print(", ".join("%s: %s" % item for item in attrs.items()))

        def _parse_options(self, raw_options):
            options = {}
            for option in raw_options:
                o = Subject.Initiate.Option(option)
                options[o.option_id] = o
            return options

        def join(self, answer, Feedback_id, option_id=None):
            answer_array = []
            if option_id or self.subtype == "essay":
                option = self.options.get(int(option_id or -1))
                if option is None or option == -1:
                    option_id = None
                    option_title = None
                else:
                    option_title = option.title.strip()
                answer_array.append(
                    [
                        subject_id,
                        Feedback_id,
                        self.Initiate_id,
                        option_id,
                        self.title.strip(),
                        option_title,
                        answer.strip(),
                    ]
                )
            else:
                for oid, option in self.options.items():
                    if hasattr(option, "other"):
                        continue
                    if answer == option.value:
                        option_value = "Yes"
                    else:
                        option_value = None
                    answer_array.append(
                        [
                            subject_id,
                            Feedback_id,
                            self.Initiate_id,
                            option.option_id,
                            self.title.strip(),
                            option.title.strip(),
                            option_value,
                        ]
                    )
            return answer_array

        class Option:
            def __init__(self, data):
                self.raw_data = data
                self.option_id = data["id"]
                self.title = data["title"]["English"]
                self.value = data["value"]
                if data.get("properties") and data.get("properties").get("other") is not None:
                    self.other = data["properties"]["other"]


# COMMAND ----------

# MAGIC %md #### Pipeline

# COMMAND ----------

# DBTITLE 1,Stage 1: subjects_df
new_data = []
max_time = Subject.get_subjects_max()
subjects_df = None

print("Most recently modified subject time in ColumnStoreDB: ", max_time)

for subject in Subject.get_subjects():
    subject_modified_on = datetime.strptime(subject.get("modified_on"), "%Y-%m-%d %H:%M:%S")
    print("API says subject %s modified on %s" % (subject.get("id"), subject_modified_on))
    if max_time is None or subject_modified_on > max_time:
        print("subject %s modified after %s" % (subject.get("id"), max_time))
        new_data.append(subject)

if len(new_data) > 0:
    subjects_df = Subject.subjects_toDF(new_data)
else:
    print("No new subjects to add")

# COMMAND ----------

# DBTITLE 1,Stage 2: Feedbacks_df & answers_df
subject_ids = [x["id"] for x in Subject.get_subjects()]

Feedbacks_to_write = []
answers_to_write = []

for subject_id in subject_ids:
    Initiates = Subject.get_Initiates(subject_id)

    # start_dt is max of Feedback table if defined, otherwise MIN of created_on field of subject table
    start_dt = Subject.get_Feedbacks_max(subject_id)
    if start_dt is None:  # (in other words, backfill )
        start_dt = Subject.get_earliest_subject_created_on(subject_id)

    # the subject doesn't exist in ColumnStoreDB at all, skip this iteration
    if start_dt is None:
        print(
            "subjectID: %s doesn't exist in ColumnStoreDB at all - this is usually an old zombie, debug further manually"
            % subject_id
        )
        continue

    print(
        "subjectID: %s most recent Feedback on %s"
        % (subject_id, datetime.strftime(start_dt, "%Y-%m-%d %H:%M:%S"))
    )

    filters = [["datesubmitted", ">", datetime.strftime(start_dt, "%Y-%m-%d %H:%M:%S")]]

    Feedbacks = Subject.get_Feedbacks(subject_id, filters)

    print("Adding %s Feedbacks for subject: %s" % (len(Feedbacks), subject_id))

    for rid, r in Feedbacks.items():
        Feedbacks_to_write.append(r.to_list())
        answers_to_write += r.answers_array(Initiates)

    print("Added %s Feedbacks so far" % len(Feedbacks_to_write))

Feedbacks_df = Subject.Feedbacks_toDF(Feedbacks_to_write)
answers_df = Subject.answers_toDF(answers_to_write)

# COMMAND ----------

# MAGIC %md Stage 3: Write subject_df, Feedbacks_df, answers_df

# COMMAND ----------

if subjects_df:
    ColumnStoreDB.safe_append_df_to_table(
        subjects_df.repartition(5), "staging", "Subject_subjects", ColumnStoreDB_creds
    )

# COMMAND ----------

if Feedbacks_df:
    ColumnStoreDB.safe_append_df_to_table(
        Feedbacks_df.repartition(5), "staging", "Subject_Feedbacks", ColumnStoreDB_creds
    )

# COMMAND ----------

if answers_df:
    ColumnStoreDB.safe_append_df_to_table(
        answers_df.repartition(5), "staging", "Subject_answers", ColumnStoreDB_creds
    )

# COMMAND ----------

# MAGIC %md #### Hive Table Insert 

# COMMAND ----------

# DBTITLE 1,Stage 1: Variable initialization and DDL
target_table = "Subject_answers"
target_schema = "lol_raw"
target_env = "__DEFAULT__"

print("env : {env}".format(env=target_env))

# COMMAND ----------

# DLL
spark.sql("""
  create table if not exists `{schema}`.`{table}` (
     f map<string,string>
  )
  partitioned by (
      env string
    , dt string
  )
  row format serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
  with serdeproperties (
    'serialization.format' = '1'
  )
  stored as
  inputformat 'org.apache.hadoop.mapred.SequenceFileInputFormat'
  outputformat 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
  tblproperties (
    'owners' = 'dps-ap-airflow-rg',
    'populatedby' = 'Databricks',
    'notebookpath' = '/production/rpg/data_processing/Subject/Subject'
  )
""".format(schema=target_schema, table=target_table))

# COMMAND ----------

# DBTITLE 1,Stage 2: Transform answers_df to same format as hive table by encapsulating the non-partitioning columns into an 'f map'.
column_list = [
    "date_submitted", "Initiate_title", "option_title", "Initiate_id", "Feedback_id", 
    "option_id", "answer_value", "subject_id"
]

dt_partition_date_format = "YYYY-MM-dd"

f_map = F.create_map(list(chain(*(
    (F.lit(name), F.col(name)) for name in column_list 
)))).alias("f")

hive_answers_df = answers_df.select(f_map).withColumn("dt",  
    F.date_format(F.col("f.date_submitted"), dt_partition_date_format
))

hive_insert_cnt = hive_answers_df.count()

print("Rows being inserted into `{schema}.{table}`: {cnt}".format(schema=target_schema, table=target_table, cnt=hive_insert_cnt))

# COMMAND ----------

temp_table = "tmp_{}".format(target_table)
hive_answers_df.createOrReplaceTempView(temp_table)
spark.catalog.cacheTable(temp_table)
param_list = spark.table(temp_table).select(F.lit(target_env).alias('env'), 'dt').drop_duplicates().collect()

for row in param_list:
  print(row.env, row.dt)
  spark.sql("""
    insert into table `{schema}`.`{table}` 
    partition(env='{env}', dt='{dt}')
    select f 
    from {temp_table}
    where dt = '{dt}'
  """.format(schema=target_schema, table=target_table, temp_table=temp_table, env=row.env, dt=row.dt))

# COMMAND ----------

# MAGIC %md #### Validation

# COMMAND ----------

min_dt = sorted(param_list, key = lambda x: x.dt)[0].dt
max_dt = sorted(param_list, key = lambda x: x.dt, reverse=True)[0].dt

validation_df = spark.sql("""
    select *
    from `{schema}`.`{table}`
    where env = '{env}'
      and dt >= '{min_dt}'
      and dt <= '{max_dt}'
""".format(schema=target_schema, table=target_table, min_dt=min_dt, max_dt=max_dt,
    temp_table=temp_table, env=target_env))
validation_count = validation_df.count()

assert validation_count >= hive_insert_cnt

# COMMAND ----------

# MAGIC %md #### Test

# COMMAND ----------

"""
# This is a messy job so including the adhoc test directly in the production notebook for ref
# An example test, you can repeat this using any active subject on a historic date range
# Choose an active subject and timerange
subject_id = 2438117
dt_start = datetime(2019, 3, 24)
dt_end = datetime(2019, 3, 26)
# Get Initiates
Initiates = Subject.get_Initiates(subject_id)
# Select a historic date range
filters = [
    ["datesubmitted", ">", dt_start.strftime("%F %T")],
    ["datesubmitted", "<", dt_end.strftime("%F %T")],
]
# Get Feedbacks
Feedbacks = Subject.get_Feedbacks(subject_id, filters)
test_answers_to_write = []
test_Feedbacks_to_write = []
for rid, r in Feedbacks.items():
    test_Feedbacks_to_write.append(r.to_list())
    test_answers_to_write += r.answers_array(Initiates)
print("Number of Feedbacks from API: ", len(test_Feedbacks_to_write))
print("Number of answers from API: ", len(test_answers_to_write))
Subject_ColumnStoreDB_Feedbacks = ColumnStoreDB.query_to_df(
    spark, "select * from staging.Subject_Feedbacks", ColumnStoreDB_creds
)
Subject_ColumnStoreDB_answers = ColumnStoreDB.query_to_df(
    spark, "select * from staging.Subject_answers", ColumnStoreDB_creds
)
print(
    "Number of Feedbacks from ColumnStoreDB: ",
    Subject_ColumnStoreDB_Feedbacks.filter(
        (F.col("subject_id") == subject_id)
        & (F.col("date_submitted") > dt_start.strftime("%F"))
        & (F.col("date_submitted") < dt_end.strftime("%F"))
    )
    .select("Feedback_id")
    .distinct()
    .count(),
)
print(
    "Number of answers from ColumnStoreDB: ",
    Subject_ColumnStoreDB_answers.filter(
        (F.col("subject_id") == subject_id)
        & (F.col("date_submitted") > dt_start.strftime("%F"))
        & (F.col("date_submitted") < dt_end.strftime("%F"))
    ).count(),
)
subject_Feedbacks_ColumnStoreDB_df = Subject_ColumnStoreDB_Feedbacks.filter(
    (F.col("subject_id") == subject_id)
    & (F.col("date_submitted") > dt_start.strftime("%F"))
    & (F.col("date_submitted") < dt_end.strftime("%F"))
)
subject_answers_ColumnStoreDB_df = Subject_ColumnStoreDB_answers.filter(
    (F.col("subject_id") == subject_id)
    & (F.col("date_submitted") > dt_start.strftime("%F"))
    & (F.col("date_submitted") < dt_end.strftime("%F"))
)
subject_Feedbacks_df = Subject.Feedbacks_toDF(test_Feedbacks_to_write)
subject_answers_df = Subject.answers_toDF(test_answers_to_write)
print(
    "Number of rows in subject_Feedbacks_df but not in subject_Feedbacks_ColumnStoreDB_df",
    subject_Feedbacks_df.subtract(subject_Feedbacks_ColumnStoreDB_df).count(),
)
print(
    "Number of rows in subject_Feedbacks_ColumnStoreDB_df but not in subject_Feedbacks_df",
    subject_Feedbacks_ColumnStoreDB_df.subtract(subject_Feedbacks_df).count(),
)
print(
    "Number of rows in subject_answers_df but not in subject_answers_ColumnStoreDB_df",
    subject_answers_df.subtract(subject_answers_ColumnStoreDB_df).count(),
)
print(
    "Number of rows in subject_answers_ColumnStoreDB_df but not in subject_answers_df",
    subject_answers_ColumnStoreDB_df.subtract(subject_answers_df).count(),
)
"""
