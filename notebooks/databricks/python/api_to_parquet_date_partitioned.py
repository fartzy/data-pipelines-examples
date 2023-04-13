# Databricks notebook source
dbutils.library.installPyPI("boto3", "1.9.231")
dbutils.library.installPyPI("pyyaml")
dbutils.library.install(
    "s3://enterprise-data-hub-prod/libs/python/datahub_spark/latest.egg"
)
dbutils.library.restartPython()

# COMMAND ----------

import json
import re
import requests

from pyspark.sql import Row
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from datahub_spark.common.glue import create_or_update_partition
from datahub_spark.common.persistence import write_to_parquet

# COMMAND ----------

API_KEY = dbutils.secrets.get("responses", "informatics_api")
informaticS_URL = "https://widgetgames.ca1.informatics.com/API/v3"

dbutils.widgets.text("partition_date", "2019-06-01")
partition_date = dbutils.widgets.get("partition_date")

tasks_per_cluster = 1024
spark.conf.set(
    "spark.sql.shuffle.partitions", tasks_per_cluster
)

log = []

# COMMAND ----------

try:
    basestring
except NameError:
    basestring = str


def get_list_of_responses():
    response_id_list = []
    url = "{}/responses".format(informaticS_URL)
    header = {"X-API-TOKEN": API_KEY}
    while url:
        r = requests.get(url=url, headers=header)
        raw_response = r.json()
        for response in raw_response["result"]["elements"]:
            response_id_list.append(response["id"])
        # Paginates through response list until there are no more pages
        url = raw_response["result"]["nextPage"]
    return response_id_list


def remove_html_tags(text):
    clean = re.compile("<.*?>")
    return re.sub(clean, "", text)


# COMMAND ----------


def get_response_information(response_id):
    url = "{}/responses/{}".format(informaticS_URL, response_id)
    header = {"X-API-TOKEN": API_KEY}
    r = requests.get(url=url, headers=header)
    raw_response = r.json()
    return raw_response


def create_questions_df(response_info_list):
    response_question_rows = []
    for response_info in response_info_list:
        response_id = response_info["result"]["id"]
        questions = response_info["result"]["questions"]
        for question in questions:
            question_id, question_text, question_type, selector, subselector = (
                    [None] * 5
            )
            informatics_id = question
            question_info = questions[question]
            if "questionName" in question_info:
                question_id = question_info["questionName"]
            if "questionText" in question_info:
                question_text = remove_html_tags(
                    question_info["questionText"]
                )
            if "questionType" in question_info:
                question_type = question_info[
                    "questionType"
                ]["type"]
                selector = question_info["questionType"][
                    "selector"
                ]
                subselector = question_info["questionType"][
                    "subSelector"
                ]
            response_question_rows.append(
                Row(
                    question_id=question_id,
                    informatics_id=informatics_id,
                    question_text=question_text,
                    question_type=question_type,
                    selector=selector,
                    subselector=subselector,
                    response_id=response_id,
                    partition_date=partition_date,
                )
            )
            # for directedasks
            directedasks = question_info.get("directedasks")
            if directedasks:
                superquestion_text = "{} ".format(
                    question_text
                )
                for directedask in directedasks:
                    question_id = "{}_{}".format(
                        question_id, directedask
                    )
                    informatics_id = "{}_{}".format(
                        question, directedask
                    )
                    question_text = (
                            superquestion_text
                            + remove_html_tags(
                        question_info["directedasks"][
                            directedask
                        ]["choiceText"]
                    )
                    )
                    response_question_rows.append(
                        Row(
                            question_id=question_id,
                            informatics_id=informatics_id,
                            question_text=question_text,
                            question_type=question_type,
                            selector=selector,
                            subselector=subselector,
                            response_id=response_id,
                            partition_date=partition_date,
                        )
                    )
    schema = StructType(
        [
            StructField("question_id", StringType(), False),
            StructField(
                "informatics_id", StringType(), False
            ),
            StructField(
                "question_text", StringType(), False
            ),
            StructField(
                "question_type", StringType(), False
            ),
            StructField("selector", StringType(), True),
            StructField("subselector", StringType(), True),
            StructField("response_id", StringType(), False),
            StructField(
                "partition_date", StringType(), False
            ),
        ]
    )
    df = sqlContext.createDataFrame(
        response_question_rows, schema=schema
    )
    return df


# COMMAND ----------

response_id_list = get_list_of_responses()
response_info_list = []
for response_id in response_id_list:
    response_info_list.append(
        get_response_information(response_id)
    )
df = create_questions_df(response_info_list)

# COMMAND ----------

log += write_to_parquet(
    df,
    "response_raw",
    "informatics_questions",
    "partition_date",
    partition_date,
)

# COMMAND ----------

create_or_update_partition(
    "response_raw",
    "informatics_questions",
    [partition_date],
    "s3://enterprise-data-hub-prod/response/raw/informatics_questions/partition_date={}".format(
        partition_date
    ),
)

# COMMAND ----------

df.unpersist()
dbutils.notebook.exit(json.dumps(log))
