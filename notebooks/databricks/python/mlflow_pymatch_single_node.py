# Databricks notebook source
control_count_filtered_df = spark.sql("""
  SELECT PTID
    , CS
    , BIRTH_YR
    , YOB
    , INDEX_DT
    , ELIG_ST
    , ELIG_END
    , GENDER
    , RACE
    , ETHNICITY
    , REGION
    , DIVISION
    , AVG_HH_INCOME
    , PCT_COLLEGE_EDUC
    , DECEASED_INDICATOR
    , PROVID_PCP
    , IDN_INDICATOR
    , FIRST_MONTH_ACTIVE
    , LAST_MONTH_ACTIVE
    , NOTES_ELIGIBLE
    , HAS_NOTES
    , SOURCEID
    , BMI
    , HT_WT
    , CKMB
    , CK
    , LPA
    , TNT_TNI
    , TNI
    , TNT
    , CASE 
        WHEN ETHNICITY =  'Not Hispanic' THEN 0
        WHEN ETHNICITY =  'Unknown' THEN 0
        WHEN ETHNICITY =  'Hispanic' THEN 1
        ELSE 0
      END AS ETH_HISP
    , CASE 
        WHEN ETHNICITY =  'Not Hispanic' THEN 0
        WHEN ETHNICITY =  'Unknown' THEN 1
        WHEN ETHNICITY =  'Hispanic' THEN 0
        ELSE 0
      END AS ETH_UNKNOWN
    , CASE 
        WHEN RACE = 'Caucasian' THEN 1
        ELSE 0 
      END AS RACE_CA
    , CASE 
        WHEN RACE = 'African American' THEN 1
        ELSE 0 
      END AS RACE_AA
    , CASE 
        WHEN RACE = 'Asian' THEN 1
        ELSE 0 
      END AS RACE_AS
    , CASE 
        WHEN RACE = 'Other/Unknown' THEN 1
        ELSE 0 
      END AS RACE_OT
    , CASE 
        WHEN GENDER = 'Female' THEN 1
        ELSE 0 
      END AS GEND_FE
    , CASE 
        WHEN REGION = 'Midwest' THEN 1
        ELSE 0 
      END AS REGION_MW
    , CASE 
        WHEN REGION = 'South' THEN 1
        ELSE 0 
      END AS REGION_SO
    , CASE
        WHEN REGION = 'West' THEN 1
        ELSE 0 
      END AS REGION_WE
    , CASE 
        WHEN REGION = 'Northeast' THEN 1
        ELSE 0 
      END AS REGION_NE
    , CASE 
        WHEN DIVISION = 'Mountain' THEN 1
        ELSE 0 
      END AS DIV_MN
    , CASE 
        WHEN DIVISION = 'East North Central' THEN 1
        ELSE 0 
      END AS DIV_ENC
    , CASE 
        WHEN DIVISION = 'Middle Atlantic' THEN 1
        ELSE 0 
      END AS DIV_MA
    , CASE 
        WHEN DIVISION = 'East South Central' THEN 1
        ELSE 0 
      END AS DIV_ESC
    , CASE 
        WHEN DIVISION = 'Pacific' THEN 1
        ELSE 0 
      END AS DIV_PAC
    , CASE 
        WHEN DIVISION = 'West North Central' THEN 1
        ELSE 0 
      END AS DIV_WNC
    , CASE 
        WHEN DIVISION = 'South Atl/West South Crl' THEN 1
        ELSE 0 
      END AS DIV_SAWSC
    , CASE 
        WHEN DIVISION = 'New England' THEN 1
        ELSE 0 
      END AS DIV_NE
    , NTILE(4) OVER ( ORDER BY AVG_HH_INCOME ) AS HH_INCOME_QTILE
    , NTILE(4) OVER ( ORDER BY COALESCE(PCT_COLLEGE_EDUC, 0) ) AS COLLEGE_EDUC_QTILE
    , CASE 
         WHEN CS = 0 THEN YEAR(INDEX_DT) 
         ELSE CAST(RIGHT(INDEX_DT, 4) AS INT) 
      END AS INDEX_DT_YR
    , SOURCE_DATA_THROUGH
    , CAST(LEFT(LAST_MONTH_ACTIVE, 4) AS INT) AS LAST_YEAR_ACTIVE
    , CAST(LEFT(LAST_MONTH_ACTIVE, 4) AS INT) AS FIRST_YEAR_ACTIVE
    , CAST(LEFT(COALESCE(DATE_OF_DEATH, '0000'), 4) AS INT) AS YEAR_OF_DEATH
  FROM optumpoc_silver.cases_controls
  WHERE 1=1
    AND BMI = 1
    AND HT_WT = 1
""")
display(control_count_filtered_df)

# COMMAND ----------

def _log_artifact(filename, file_ext, file, save_location):
  
  temp_file = tempfile.NamedTemporaryFile(prefix=filename+"-", suffix="."+file_ext)
  temp_file_name = temp_file.name
  
  try:    
    if file_ext == "csv":
      file.to_csv(temp_file_name)
      
    elif file_ext == "png":
      file.savefig(temp_file_name)
      
    mlflow.log_artifact(temp_file_name, save_location)
    
  except Exception as e:
    print(e)
    
  finally:  
    temp_file.close()
    
    
def _save_to_local_and_log_artifact(filename, file_ext, data, save_location):
    full_temp_path = "/"+filename+"."+file_ext
  
    with open(full_temp_path, "w+") as f:
      for datum in data:
        f.write(datum + "\n")
        
    mlflow.log_artifact(full_temp_path, save_location)    

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install git+https://github.com/fartzy/pymatch.git

# COMMAND ----------

import tempfile
import warnings
warnings.filterwarnings('ignore')

import cloudpickle
import databricks.koalas as ks
import mlflow
import numpy as np
import pandas as pd
from pymatch.Matcher import Matcher

# COMMAND ----------

run = mlflow.start_run()

#convert to koalas
with ks.option_context("compute.default_index_type", "distributed-sequence"):
   cases_controls_kdf = control_count_filtered_df.to_koalas()

#convert to pandas
cases_controls_pdf = cases_controls_kdf.to_pandas()

pandas_col_list = [
"INDEX_DT",
"PTID",
"CS",
"YOB",
"TNT",
 "TNI",
 "TNT_TNI",
 "LPA",
 "CK",
 "CKMB",
 "ETH_HISP",
 "ETH_UNKNOWN",
 "RACE_CA",
 "RACE_AA",
"RACE_AS",
"RACE_OT",
"GEND_FE",
"REGION_MW",
"REGION_SO",
"REGION_WE",
"REGION_NE",
#   "DIV_MN",
#   "DIV_ENC",
#   "DIV_MA",
#   "DIV_ESC",
#   "DIV_PAC",
#   "DIV_WNC",
#   "DIV_SAWSC",
#   "DIV_NE",
"HH_INCOME_QTILE",
"COLLEGE_EDUC_QTILE",
"INDEX_DT_YR",
"SOURCE_DATA_THROUGH",
"FIRST_YEAR_ACTIVE",
"LAST_YEAR_ACTIVE",
#   "YEAR_OF_DEATH",
]

exclude_cols = [
  "INDEX_DT",
  "PTID",
]

random_seed = 20200920
nmodels = 100
nmatches = 2
threshold = 0.0001
method = "min"

mlflow.log_param("exclude_cols", exclude_cols)
mlflow.log_param("yvar", "CS")
mlflow.log_param("random_seed", random_seed)
mlflow.log_param("nmodels", nmodels)
mlflow.log_param("test_set_expr", "test = cases_controls_datav2[cases_controls_datav2.CS == 1]")
mlflow.log_param("control_set_expr", "control = cases_controls_datav2[cases_controls_datav2.CS == 0]")
mlflow.log_param("fit_scores_balance", True)

# Only use specified columns from above list
# ( No need for perfect GLM but there should be at least minimal feature selection )
cases_controls_pdf = cases_controls_pdf[pandas_col_list] 

# create test and control sets
test = cases_controls_pdf[cases_controls_pdf.CS == 1]
control = cases_controls_pdf[cases_controls_pdf.CS == 0]

# This initializes the Matcher object which holds a lot of metadata 
m = Matcher(test, control, yvar="CS", exclude=exclude_cols) 

# # Grab the runID and experimentID 
runID = run.info.run_uuid
experimentID = run.info.experiment_id    

# set the seed for repr:30oducibility
np.random.seed(random_seed)

# Fits the GLM that are used to get the propensity scores 
fs = m.fit_scores(balance=True, nmodels=nmodels) #1.67 minutes

# Log the accuracy - this helps determin if its good use-case for PSM
if fs:
  mlflow.log_metric("accuracy", fs)
  
# predict propensity score for each observation 
m.predict_scores() #1.5 minutes

# 'tuning_chart' - to see acceptable `threshold` parameter 
# keep in mind that this chart needs to be looked at manually and then apply it to 
# the 'threshold' parameter above 
# You can see from the chart where the percentage levels out at 100 
tuning_chart = m.tune_threshold(method='random')

# This chart gives a visual of how the propensity scores differ from cases and controls 
propensity_chart = m.plot_scores()

# matching method can be either 'min' or 'random' 
# choosing 'min' will create the best matches but it takes longer 
# 'min' is also the default 
m.match(method="random", nmatches=nmatches, threshold=threshold) #13.33 minutes

rf = m.record_frequency()
md = m.matched_data.sort_values("match_id")
md.to_csv("/dbfs/psm/matched_data.csv", index=False)
rf.to_csv("/dbfs/psm/rf.csv", index=False)


#Log all the different data for artifacts using a temporary files
_log_artifact("control-vs-test-plot", "png", propensity_chart, "charts")
_log_artifact("tune-threshold", "png", tuning_chart, "charts")
_log_artifact("matched-data", "csv", md, "output-data")
_log_artifact("record-frequency", "csv", m.record_frequency(), "output-data")
_save_to_local_and_log_artifact("final-pandas-columns", "txt", pandas_col_list, "selected-features")
_save_to_local_and_log_artifact("final-pyspark-columns", "txt", control_count_filtered_df.columns, "selected-features")

# Log model 
#mlflow.pyfunc.log_model(m, "propensity-score-model")

# COMMAND ----------

mlflow.end_run()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT PTID
# MAGIC    , INDEX_DT
# MAGIC    , match_id
# MAGIC    , record_id
# MAGIC    , scores
# MAGIC  FROM optumpoc_silver.matched_records

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL optumpoc_silver.matched_records --129580
