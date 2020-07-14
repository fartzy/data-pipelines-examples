# Databricks notebook source
# DBTITLE 1,Imports
import json
import os
import requests
from datetime import datetime
from email import encoders
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

import boto3
from cryptography.fernet import Fernet
import quopri


# COMMAND ----------

# DBTITLE 1,Report Parameters
dbutils.widgets.text("job_names", "All-Usage,Commercial-Usage,Medical-Usage,RND-Usage,TeamA-RND-Usage")
dbutils.widgets.text("report_path", "/usage_report/executions")
dbutils.widgets.text("to_addrs", "michaeleartz@gmail.com")
dbutils.widgets.text("from_addr", "michaeleartz@gmail.com")

#Set variables
prod_jobs_api_uri = "https://acme.cloud.databricks.com/api/2.0/jobs"

job_names = dbutils.widgets.get("job_names").replace(" ", "").split(",")
job_infos_no_ids = [{'name': "{job}".format(job=job), 'job_id':0, 'last_job_run':0} for job in job_names]
to_addrs = dbutils.widgets.get("to_addrs").replace(" ", "").split(",")
from_addr = dbutils.widgets.get("from_addr")
root_path = dbutils.widgets.get("report_path")

# COMMAND ----------

# DBTITLE 1,Helper Functions
def get_http_headers():
  auth = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
  headers = {"Authorization": "Bearer {auth}".format(auth=auth)}
  return headers



def get_response_map(endpoint, headers):
  response = requests.get(endpoint, headers=headers)  
  response_json = json.loads(response.text)
  return response_json



def attach_job_ids(job_infos, headers, prod_jobs_api_uri):
  #get the relevant job_ids from the jobs that are listed in the 'job_names' widget
  endpoint = "{uri}/list".format(uri=prod_jobs_api_uri,)
  jobs_json = get_response_map(endpoint, headers)
  for job in jobs_json['jobs']:
    for i in range(len(job_infos)):
      if (job['settings']['name'] == job_infos[i]['name']):
        job_infos[i]['job_id'] = job['job_id']
  return job_infos



def get_report_path(root_path, bu):
  report_path = (root_path + "/bu={bu}/".format(bu=bu) + datetime.today().strftime('%Y-%m-%d')).lower()
  return report_path




def get_events_metadata_row(job_run_info_map, start_time, root_path):
  #extracting all the info into variables for easy insertion into the events table 
  report_exec_start_ts = job_run_info_map['start_time']
  report_exec_start = datetime.utcfromtimestamp(report_exec_start_ts//1000).replace(microsecond=report_exec_start_ts%1000*1000)
  report_exec_end_ts = (
    int(report_exec_start_ts) + 
    int(job_run_info_map['setup_duration']) +
    int(job_run_info_map['execution_duration']) + 
    int(job_run_info_map['cleanup_duration'])
  )
  
  report_exec_end = datetime.utcfromtimestamp(report_exec_end_ts//1000).replace(microsecond=report_exec_end_ts%1000*1000)
  job_run_id = job_run_info_map["run_id"]
  number_in_job = job_run_info_map["number_in_job"]
  notebook_in_job = job_run_info_map["task"]["notebook_task"]["notebook_path"]
  #default_parameters = job_run_info_map["task"]["notebook_task"]["base_parameters"]
  job_name = job_info['name']
  report_path = get_report_path(root_path, bu)
  
  if ("overriding_parameters" in job_run_info_map):
    non_default_parameters = job_run_info_map["overriding_parameters"]
  else:
    non_default_parameters = "N/A"
  
  return (
    start_time, 
    job_name, 
    report_path, 
    job_run_id, 
    number_in_job, 
    notebook_in_job, 
    non_default_parameters,
    report_exec_start, 
    report_exec_end,
  )
  
  
  
  
def save_to_s3(report_path, html_body):
  try:
    dbutils.fs.put("{report_path}/usage_report.html".format(report_path=report_path), html_body)
  except:
    print("Put failed. Is {report_path}/usage_report.html already there?".format(report_path=report_path))
    
    
    
    
def send_email(to_addrs, from_addr, subject, html_body, run_id,bu,date):
  msg = MIMEMultipart()
  msg['Subject'] = "Databricks usage metrics for {bu} {date}".format(bu=bu, date=date)
  msg['From'] = from_addr
  msg['To'] = ", ".join(to_addrs)
  
  
  filename="databricks_usage_job_business-unit={bu}_{date}.html".format(bu=bu, date=date)
  temp_filepath = "/dbfs/tmp/{run_id}.html".format(run_id=run_id)
  
  with open(temp_filepath, "wb+") as f:
    f.write(email_body.encode())
    
  attachment = MIMEApplication(open(temp_filepath, 'rb').read())
  attachment.add_header('Content-Disposition', 'attachment', filename=filename)
  msg.attach(attachment)
  
  ses = boto3.client('ses', region_name='us-east-1')
  ses.send_raw_email(
    Source=msg['FROM'],
    Destinations=to_addrs,
    RawMessage={'Data': msg.as_string()}
  )  
   
  os.remove(temp_filepath)
  print("Sending Email.")    
  

# COMMAND ----------

# DBTITLE 1,Iterate Through Jobs
headers = get_http_headers()
job_infos = attach_job_ids(job_infos_no_ids, headers, prod_jobs_api_uri)
executions_list =[]  



for job_info in job_infos:
  
  #beginning of execution of report exporting
  html_export_start_time = datetime.now()
  
  #today's date
  date = datetime.today().strftime('%Y-%m-%d')
  
  #get jobid and business unit of the job run 
  job_id = job_info['job_id']
  bu = job_info['name'].replace("-Usage", "")
  
  #The first API call gets the latest run_id of the above job 
  #loop needs to iterate until finding a job run that is a periodic run
  offset_counter = 1
  while True:
    endpoint = "{uri}/runs/list?job_id={job_id}&active_only=false&offset={offset_counter}&limit=1".format(
      job_id=job_id, 
      uri=prod_jobs_api_uri,
      offset_counter=str(offset_counter)
    )
    one_time_or_periodic_run = get_response_map(endpoint, headers)['runs'][0]['trigger']
    if not one_time_or_periodic_run == 'ONE_TIME':
      break
    offset_counter = offset_counter + 1
     
  job_info['last_job_run'] = get_response_map(endpoint, headers)['runs'][0]['run_id']
  
  #The second API call gets the job output html which will be saved in to S3 location and emailed 
  endpoint = "{uri}/runs/export?run_id={run_id}".format(
    run_id=job_info['last_job_run'],
    uri=prod_jobs_api_uri,
  )
  
  email_body = get_response_map(endpoint, headers)['views'][0]['content'] 


  
  #The third API call gets the job info about the latest job run to be inserted into Report_Executions table that keeps history
  endpoint = "{uri}/runs/get?run_id={run_id}".format(
    run_id=job_info['last_job_run'],
    uri=prod_jobs_api_uri,
  )   
  job_run_gets_json = get_response_map(endpoint, headers)
  
  executions_row = get_events_metadata_row(
    job_run_gets_json, 
    html_export_start_time,
    root_path,
  )
  
  #add the row (python tuple) of data to the list
  executions_list.append(executions_row)
  
  #save to s3
  save_to_s3(executions_row[2], email_body)
  
  subject = "Databricks usage metrics for {bu} {date}".format(bu=bu, date=date)
  
  send_email(
    to_addrs, 
    from_addr, 
    subject, 
    email_body, 
    job_info['last_job_run'], 
    bu, 
    date,
  )

# COMMAND ----------

# DBTITLE 1,Write to Report_Executions Table
#create DataFrame from the list of executions and write to the delta location   
tbl_location = "/usage_report/executions/delta"

output_df = spark.createDataFrame(executions_list).toDF(
  "report_export_execution_time", 
  "databricks_job_name", 
  "report_s3_export_path", 
  "job_run_id",
  "number_in_job", 
  "notebook_in_job", 
  "non_default_parameters", 
  "report_execution_start", 
  "report_execution_end",
)

output_df.write.format("delta").mode("append").save(tbl_location)

spark.sql("CREATE TABLE IF NOT EXISTS report_executions USING DELTA LOCATION '{tbl_location}'".format(tbl_location=tbl_location))
