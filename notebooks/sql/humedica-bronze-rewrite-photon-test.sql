-- Databricks notebook source
--enbales dbio cache which is good to have and not on by default unless it is ix3 type ec2 node
set spark.databricks.io.cache.enabled = "true"
set spark.databricks.io.cache.compression.enabled = "false"

--speed up the way delta checks for metadata
set spark.databricks.delta.checkpointV2.enabled = "false"
set spark.databricks.delta.fastQueryPath.enabled = "false"

--speeds up the way delta handles the bin packed files
set spark.databricks.delta.fastQueryPath.dataskipping.enabled = "false"

--specifies the size of input partitions
set spark.sql.files.maxPartitionBytes = 1024 * 1024 * 32 

--default of 200 is too low
set spark.sql.shuffle.partitions = 2560

--for adaptive query execution (AQE) new in spark 3.0 (DBR 7.0)
--by default I set it to true usually 
set spark.sql.adaptive.enabled = "false"

set spark.sql.adaptive.skewJoin.enabled = "true"
set spark.sql.adaptive.coalescePartitions.enabled = "true"
set spark.sql.adaptive.coalescePartitions.minPartitionNum = 640
set spark.databricks.adaptive.autoBroadcastJoinThreshold = "50m"

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vw1 AS SELECT id FROM RANGE(16);

-- COMMAND ----------

-- MAGIC %md ## CDM Usecase
-- MAGIC * Transpose large table
-- MAGIC * Join large tables
-- MAGIC * Eliminate Duplicates
-- MAGIC 
-- MAGIC *SAS CODE*
-- MAGIC <pre>
-- MAGIC 
-- MAGIC libname hume "/root/data_00/medical/2014Q4/data";
-- MAGIC libname temp "/root_scratch/medical_mscdm";
-- MAGIC 
-- MAGIC options obs=1000000000 fullstimer compress=yes fmtsearch=(hume);
-- MAGIC 
-- MAGIC 
-- MAGIC data start;
-- MAGIC st_dttm=datetime(); format st_dttm datetime.;
-- MAGIC put st_dttm;
-- MAGIC run;
-- MAGIC 
-- MAGIC *the actual file contains 10.4 billion records-limiting to the first 1b here;
-- MAGIC proc sql;
-- MAGIC create table dx as
-- MAGIC select a.*, a.diagnosis_cd as dx1, 
-- MAGIC a.diagnosis_cd as dx2,
-- MAGIC a.diagnosis_cd as dx3, 
-- MAGIC a.diagnosis_cd as dx4, 
-- MAGIC a.diagnosis_cd as dx5, 
-- MAGIC a.diagnosis_cd as dx6, 
-- MAGIC a.diagnosis_cd as dx7, 
-- MAGIC a.diagnosis_cd as dx8, 
-- MAGIC a.diagnosis_cd as dx9, 
-- MAGIC a.diagnosis_cd as dx10, 
-- MAGIC a.diagnosis_cd as dx11, 
-- MAGIC a.diagnosis_cd as dx12, 
-- MAGIC a.diagnosis_cd as dx13, 
-- MAGIC a.diagnosis_cd as dx14, 
-- MAGIC a.diagnosis_cd as dx15, 
-- MAGIC a.diagnosis_cd as dx16
-- MAGIC from hume.diagnosis a ;
-- MAGIC quit;
-- MAGIC 
-- MAGIC options obs=max;
-- MAGIC 
-- MAGIC *attach a provider id to it-DB can nest this if they want;
-- MAGIC 
-- MAGIC proc sql;
-- MAGIC          create table provct1
-- MAGIC          as select b.encid, max(provid) as provid
-- MAGIC          from dx a inner join hume.enc_provider b
-- MAGIC 		 on a.encid=b.encid 
-- MAGIC          group by 1 ;
-- MAGIC quit;
-- MAGIC 
-- MAGIC 
-- MAGIC proc sql;
-- MAGIC create table dx2
-- MAGIC as select a.*, b.provid
-- MAGIC from
-- MAGIC dx a left join provct1  b
-- MAGIC on a.encid=b.encid ;
-- MAGIC 
-- MAGIC drop table dx;
-- MAGIC 
-- MAGIC drop table provct1;
-- MAGIC quit;
-- MAGIC 
-- MAGIC 
-- MAGIC *attach encounter type-they can nest this section too;
-- MAGIC proc sql;
-- MAGIC create table dx3
-- MAGIC as select a.*, 
-- MAGIC (case when b.interaction_type in ('APS','DSP','OFF','URG') then 'AV'
-- MAGIC       when b.interaction_type in ('ER') then 'ED'
-- MAGIC 	  when b.interaction_type in ('INP','IPS','IRE') then 'IP'
-- MAGIC 	  when b.interaction_type in ('HOM','HPC','NH','OBS','SNF','SWG','LTC') then 'IS'
-- MAGIC 	  when b.interaction_type in ('IMG','LAB','LTR','OPA','PAL','RX','TEL','OPT','IMG','NR','UN','OO','ORR','TOL','UNK') then 'OA'
-- MAGIC else '' end) as enctype
-- MAGIC from dx2 a left join hume.encounter b
-- MAGIC on a.ptid=b.ptid & a.encid=b.encid;
-- MAGIC quit;
-- MAGIC 
-- MAGIC proc delete data=dx2; quit;
-- MAGIC 
-- MAGIC *expand from horizontal to vertical-this would be absolutely massive-they can develop the sas analogue;
-- MAGIC data diagnosis (drop=dx1-dx16);
-- MAGIC set dx3;
-- MAGIC 
-- MAGIC 
-- MAGIC    array diag{*} dx:;
-- MAGIC     do i=1 to dim(diag);
-- MAGIC     if diag[i] ne  ""  then do;
-- MAGIC           
-- MAGIC           dx = diag[i];
-- MAGIC           output diagnosis;
-- MAGIC         end;
-- MAGIC    end;
-- MAGIC run;
-- MAGIC 
-- MAGIC proc delete data=dx3; quit;
-- MAGIC 
-- MAGIC *deduplicate table on the following key -let DB do this either using a windowing function or
-- MAGIC  some other equivalent option - anything other than simple distinct command-this is a 
-- MAGIC  deduplication that must occur on no more and no less the 6 variables listed;
-- MAGIC 
-- MAGIC proc sort nodupkey data=diagnosis out=final_diagnosis; by ptid provid encid dx diag_date enctype; run;
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC data end;
-- MAGIC end_dttm=datetime(); format end_dttm datetime.;
-- MAGIC put end_dttm;
-- MAGIC run;
-- MAGIC 
-- MAGIC data huh;
-- MAGIC merge start end;
-- MAGIC diff=end_dttm-st_dttm;
-- MAGIC format diff hhmm5.;
-- MAGIC put 'total time elapsed is: '  diff;
-- MAGIC run;
-- MAGIC  
-- MAGIC 
-- MAGIC x "echo 'finished' | mailx -s '1 billion row test Humedica POC complete' email@email.com";
-- MAGIC </pre>

-- COMMAND ----------

-- DBTITLE 1,Spark SQL: Whole Query
CREATE OR REPLACE TEMP VIEW final_vw AS 
WITH dx1 AS (
    SELECT a.*, 
    a.diagnosis_cd as dx1, 
    a.diagnosis_cd as dx2,
    a.diagnosis_cd as dx3, 
    a.diagnosis_cd as dx4, 
    a.diagnosis_cd as dx5, 
    a.diagnosis_cd as dx6, 
    a.diagnosis_cd as dx7, 
    a.diagnosis_cd as dx8, 
    a.diagnosis_cd as dx9, 
    a.diagnosis_cd as dx10, 
    a.diagnosis_cd as dx11, 
    a.diagnosis_cd as dx12, 
    a.diagnosis_cd as dx13, 
    a.diagnosis_cd as dx14, 
    a.diagnosis_cd as dx15, 
    a.diagnosis_cd as dx16,
    cast(rand() * 16 as int) AS id
    FROM demo_database.diagnosis a  
),

enc_distinct_prov_skewed AS (
    SELECT encid
        , max(provid) as provid
    FROM demo_database.encounterprovider b
    GROUP BY encid
),

enc_distinct_prov AS (
    SELECT f.encid
        , f.provid
        , v.id
    FROM enc_distinct_prov_skewed f
    CROSS JOIN vw1 v
),

dx2 AS (
    SELECT c.*
        , d.provid
    FROM dx1 c
    LEFT JOIN enc_distinct_prov d 
    ON c.encid = d.encid
        AND c.id = d.id
),

dx3 AS (
    SELECT a.*, 
        CASE 
        WHEN b.interaction_type IN ('Ambulatory patient services','Day surgery patient','Office or clinic patient','Urgent care') THEN 'AV'
        WHEN b.interaction_type IN ('Emergency patient') THEN 'ED'
        WHEN b.interaction_type IN ('Inpatient','Inpatient psych','Inpatient rehab') THEN 'IP'
        WHEN b.interaction_type IN ('Home visits','Hospice','Nursing home / Assisted living','Observation patient','Skilled nursing facility inpatient','Swing bed','Long term acute care') THEN 'IS'
        WHEN b.interaction_type IN ('Imaging','Labs and diagnostics.nonimaging','Letter / Email','Other physician advice','Palliative care',
            'Prescriptions and refills','Telephone / Online','Other patient type', 'Not recorded','UN','Other orders','Other Reports / Results', 'Unknown patient type') THEN 'OA'
        ELSE '' 
        END AS enctype
    FROM dx2 a 
    LEFT JOIN demo_database.encounter b
    ON a.ptid = b.ptid 
        AND a.encid = b.encid
),

unpivotdx AS (
    SELECT PTID
        , ENCID
        , PROVID
        , DIAG_DATE
        , DIAG_TIME
        , DIAGNOSIS_CD
        , DIAGNOSIS_STATUS
        , POA
        , ADMITTING_DIAGNOSIS
        , DISCHARGE_DIAGNOSIS
        , PRIMARY_DIAGNOSIS
        , PROBLEM_LIST
        , SOURCEID
        , ENCTYPE
        , stack(16, dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12, dx13, dx14, dx15, dx16) as DX
    FROM dx3
)

SELECT *
FROM unpivotdx
WHERE dx IS NOT NULL

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC  (
-- MAGIC    spark
-- MAGIC      .read
-- MAGIC      .table("final_vw")
-- MAGIC      .dropDuplicates(['PTID', 'PROVID', 'ENCID', 'DX', 'DIAG_DATE', 'ENCTYPE'])
-- MAGIC      .write
-- MAGIC      .saveAsTable(
-- MAGIC        'demo_database.final_diagnosis_full_photon',
-- MAGIC        format='delta',
-- MAGIC        mode='overwrite',
-- MAGIC        path='ss3://bucket-name-1/data/demo_database.db/final_diagnosis_full_photon'
-- MAGIC      )
-- MAGIC  )
