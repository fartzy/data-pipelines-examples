import json
import logging
import os
from datetime import datetime

import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow import configuration as conf
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from python_graphql_client import GraphqlClient

from dags.utils import demodb

ARGS = {
    "description": "graphql__teams",
    "owner": "user@email.com",
    "depends_on_past": False,
    "email": ["martz@email.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

dag_name = "graphql__teams_headcount"

dag = DAG(
    dag_name,
    default_args=ARGS,
    start_date=datetime(2020, 1, 1),
    catchup=False,
    schedule_interval="@monthly",
)


def main():

    demo_DATABASE_SCHEMA = "raw"
    demo_DATABASE_TABLE = "graphql__teams_headcount"

    # GraphQL credential setup
    demo_GRAPHQL_API_CREDENTIALS = BaseHook.get_connection("demo_graphql_api_token")
    GRAPHQL_HEADERS = {
        "Authorization": f"Bearer {demo_GRAPHQL_API_CREDENTIALS.password}",
        "Content-type": "application/json",
    }
    GRAPHQL_BASE_URL = f"https://{demo_GRAPHQL_API_CREDENTIALS.host}"

    # Instantiate the client with the Acme GraphQL endpoint
    client = GraphqlClient(endpoint=f"{GRAPHQL_BASE_URL}/api/v1/graphql", headers=GRAPHQL_HEADERS)

    # graphQL query for the request to the above api endpoint
    query = """
        {
            group
            {
                team_id : scope
                headcount
            }
        } 
    """

    # Synchronous request
    res = client.execute(query=query,)["data"]["group"]

    # prep list of dicts for pscycop insert
    insert_vals = [(tm["team_id"], tm["headcount"],) for tm in res]

    # Open a connection to the demo database
    with demodb.get_connection() as demodb_connection:
        with demodb_connection.cursor() as cur:
            args = ",".join(cur.mogrify("(%s,%s)", x).decode("utf8") for x in insert_vals)

            cur.execute(
                f"""
                  insert into {demo_DATABASE_SCHEMA}.{demo_DATABASE_TABLE} (team_id, name) values 
                  {args}
                """
            )


# Operator/Task Declaration
latest_only = LatestOnlyOperator(task_id="latest_only", dag=dag)
create_tables = PostgresOperator(
    task_id="graphql__teams_headcount_create_tables",
    postgres_conn_id="demo_data_db",
    dag=dag,
    auto_commit=True,
    sql=[
        """
        create table if not exists raw.graphql__teams_headcount (
            headcount int
          , team_id varchar(200)
          , loaded_to_conformed boolean default false
          , insertion_datetime timestamptz default now()
        )      
      """,
        """
        create table if not exists conform.graphql__teams_headcount (
            id serial primary key
          , headcount int
          , team_id varchar(200) references conform.graphql__teams(id)
          , active boolean default true
          , insertion_datetime timestamptz default now()
          , unique(team_id, insertion_datetime)
        )      
      """,
    ],
)
trasform_to_conformed = PostgresOperator(
    task_id="conform.graphql__teams_headcount",
    postgres_conn_id="demo_data_db",
    auto_commit=True,
    sql=[
        """
      with latest as (
        select * 
        from raw.graphql__teams_headcount
        where loaded_to_conformed = false
      )

      select headcount
        , team_id
      into temp temp_graphql__teams_headcount
      from latest 
    """,
        """
      insert into conform.graphql__teams_headcount (headcount, team_id) 
      select id
        , words
      from temp_graphql__teams_headcount thc
      where not exists (
        select 1
        from conform.graphql__teams_headcount cgth
        where cgth.team_id = thc.team_id
          and cgth.headcount = thc.headcount
          and cgth.active is true
        )
    """,
        """
      with update_headcount as (
        select rank() over (partition by team_id order by insertion_datetime desc) as recent_rank
          , team_id
          , insertion_datetime
          , active
        from conform.graphql__teams_headcount
      )

      update conform.graphql__teams_headcount cth
      set active = false 
      from update_headcount uh 
      where uh.team_id = cth.team_id
        and uh.insertion_datetime = cth.insertion_datetime
        and uh.recent_rank > 1
    """,
        """
      update raw.graphql__teams_headcount
      set loaded_to_conformed = true
    """,
    ],
)
primary = PythonOperator(task_id="raw.graphql__teams_headcount", python_callable=main, dag=dag)

latest_only >> create_tables >> primary >> trasform_to_conformed
