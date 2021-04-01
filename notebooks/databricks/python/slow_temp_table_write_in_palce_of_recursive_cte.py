# Databricks notebook source
# MAGIC %md
# MAGIC ## Eligibility data - collapsing rows
# MAGIC 1. The script below will join data onto itself N number of times. N being the maximum number of distinct rows with the same`memnum` 
# MAGIC 2. After the final interim result set is made, the final result will be the aggregation of that.  The aggregation will be on `memnum` and a derived column `group_num`
# MAGIC 3. There is a looping step that decides the `group_num` within a `memnum`. Kind of based off of the concept of `Recursive Common Table Expression (CTE)`
# MAGIC    *  This is not truely a `Recursive CTE` because they are not allowed in Spark SQL yet
# MAGIC      * Check out the below [SQL Statement](https://biib.cloud.databricks.com/#notebook/376403/command/377439) to see what this will look like once `Recursive CTE` is allowed in Spark SQL
# MAGIC      * There is actually work just completed to merge `Recursive CTE` into Spark 3.1 [Check it out](https://github.com/apache/spark/pull/29210)
# MAGIC         * This could make it into a future DBR version *very* soon! Especially if they do merge it into Spark 3.1
# MAGIC 4. The script takes **45-90 seconds** to run but the time might be much more for a larger data set, or with a greater max number of distinct rows with the same`memnum`
# MAGIC 5. I do not claim that this cannot be done using traditional set based logic. I am going to look at that as well

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM eligibility_pre a

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.memnum  a_memnum 
# MAGIC      , a.begindt a_begindt
# MAGIC      , a.enddt   a_enddt
# MAGIC      , b.memnum b_memnum
# MAGIC      , b.begindt b_begindt
# MAGIC      , b.enddt b_enddt
# MAGIC FROM eligibility_pre a
# MAGIC   JOIN eligibility_pre b
# MAGIC     ON a.memnum = b.memnum 
# MAGIC     AND ( 
# MAGIC         a.begindt <= b.enddt 
# MAGIC       OR 
# MAGIC         b.begindt <= a.enddt
# MAGIC       )
# MAGIC     AND (
# MAGIC       a.begindt < b.enddt
# MAGIC       OR
# MAGIC       b.begindt < a.enddt
# MAGIC     )
# MAGIC ORDER BY 1, 2

# COMMAND ----------

from pyspark.sql.functions import asc

# A delta table is used write and rewrite the results of every incremental result set. `Recursive` 
# here just means that the result set keeps joining unto itself until all of the rows from the original 
# `eligibility_pre` table have been passed through. 
temp_delta_path = "/temp/recursivetable/delta"
view_name = "recursive_cte"

# Just to check to make sure there is no files in whatever we choose as the temp delta path 
# This might cause an error if the directory is not empty 
dbutils.fs.rm(temp_delta_path, True)

# The first part - `distinct_records` - is to get just the distinct records because keeping the duplicates  
# does not change the result anyway. The second part of the below common table expression (CTE) is so that 
# it ranks every row within a memnum by ascending begindt
windowed_by_memnum_and_ranked_by_begindt = spark.sql("""
  WITH distinct_records AS (
    SELECT DISTINCT * 
    FROM eligibility_pre
  )
  ,
  ranked_by_begindt AS (
    SELECT memnum
       , begindt
       , enddt
       , ROW_NUMBER() OVER ( PARTITION BY memnum ORDER BY begindt ASC) AS memnum_rk
    FROM distinct_records
    ORDER BY memnum ASC
      , begindt ASC
  )

  SELECT * 
  FROM ranked_by_begindt
""")

# Create a temp view that can be referenced later in the recursive part of this query below 
# (the looping is during the while loop)
windowed_by_memnum_and_ranked_by_begindt.createOrReplaceTempView("cte")

# The `anchor row` of the recursion which is the first row. This is also called the `base case`. Each subsequent 
# row will be linked to only one 'anchor' row an 'anchor' row is the earliest begindt row of every memnum
anchor_records_of_recursive_cte = spark.sql("""
  SELECT *
      , 0 AS group_num
      , enddt AS local_maxenddt
  FROM cte
  WHERE memnum_rk = 1 
""")

# Just persisting to s3 the anchor set
(
  anchor_records_of_recursive_cte
    .write
    .mode("overwrite")
    .format("delta")
    .save(temp_delta_path)
)

while True:
  
  before_union_df = spark.read.format("delta").load(temp_delta_path)
  num_rows_before = before_union_df.count() 
  before_union_df.createOrReplaceTempView(view_name)
  
  unioned_df = spark.sql("""
    /* 
    *
    *  Pull from the "previous" recursive_cte that will be unioned to the join query. The second query causes the result 
    *  set to pick up more rows from the above recursive_cte until all the rows have been joined or the join criteria 
    *  is no longer met. The UNION ensures there will be no duplicates.
    *
    */
    SELECT *
    FROM {view_name}

    UNION 

    SELECT c.memnum
       , c.begindt
       , c.enddt
       , c.memnum_rk AS memnum_rk
       
       /* 
       *  
       *  If there is a day (or less) between the previous row `enddt` and the current row `begindt`, or there is a day 
       *  (or less) between the `local_maxendt` and the current row `begindt`, then the same group number will be kept. 
       *  This is so the aggregation later on in line 145 in the `collapsed` dataframe. With this grouping column, we 
       *  can group by for the correct min() and max() dates.
       *
       */
       , CASE 
           WHEN DATEDIFF(c.begindt, recur.enddt) <= 1 OR DATEDIFF(c.begindt, recur.local_maxenddt) <= 1 
             THEN recur.group_num
           ELSE recur.group_num + 1
         END AS group_num
         
       /*
       *
       *  Just because a row has an earlier begindt, does not mean that it cannot have a later enddt. In other words,
       *  An earlier row can have a greater enddt than a later row. However, the rows should all be collapsed if there 
       *  was an earlier row with a later enddt. A good exmaple of this is below where row 1 actually has a greater 
       *  end date than row 2 in poc00000007 -
       *               memnum     begindt      enddt
       *    row 1 - poc00000007	2018-05-01	2019-09-30 ---- enddt is later than below enddt
       *    row 2 - poc00000007	2018-10-01	2018-12-31
       *    row 3 - poc00000007	2019-01-01	2019-08-31
       *
       */
       , CASE 
           WHEN ( DATEDIFF(c.begindt, recur.enddt) <= 1 OR DATEDIFF(c.begindt, recur.local_maxenddt) <= 1 ) 
             AND DATEDIFF(recur.local_maxenddt, c.enddt) > 0 
               THEN recur.local_maxenddt
           WHEN ( DATEDIFF(c.begindt, recur.enddt) <= 1 OR DATEDIFF(c.begindt, recur.local_maxenddt) <= 1 ) 
             AND DATEDIFF(recur.local_maxenddt, c.enddt) <= 0 
               THEN c.enddt
           WHEN DATEDIFF(c.begindt, recur.enddt) > 1 AND DATEDIFF(c.begindt, recur.local_maxenddt) > 1 
             THEN c.enddt
         END AS local_maxenddt
    FROM {view_name} recur 
    
    /* 
    *
    *  The join here `c.memnum_rk = recur.memnum_rk + 1` ensures that each row from `cte` is joined to the row with the 
    *  previous `begindt` with the same `memnum`.  The max amount of iterations is max amount of unique rows within a 
    *  `memnum` window
    *
    */
    JOIN cte c 
      ON c.memnum_rk = recur.memnum_rk + 1
        AND c.memnum = recur.memnum
  """.format(view_name=view_name))
  
  unioned_df.write.mode("overwrite").format("delta").save(temp_delta_path)
  num_rows_after = spark.read.format("delta").load(temp_delta_path).count()

  # Exit the recursion. If there are no new rows added then we can say then the looping is complete
  if (num_rows_after == num_rows_before):
    break

spark.read.format("delta").load(temp_delta_path).createOrReplaceTempView(view_name)

# Final aggregation to get the collapsed result set. The `group_num` column is really the result of the 
# recursive_cte that matters
collapsed = sql("""
  SELECT memnum
    , MIN(begindt) AS eligeff_coll
    , MAX(enddt) AS eligend_coll
  FROM {view_name} 
  GROUP BY memnum 
    , group_num 
  ORDER BY memnum ASC
    , eligeff_coll ASC
""".format(view_name=view_name))

display(collapsed)
temp_delta_result_path = "/temp/recursivetable/result/delta"
collapsed.write.mode("overwrite").format("delta").save(temp_delta_result_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This does not execute in Spark SQL yet (as of 2020-08-06).  
# MAGIC -- This dialect below is PostgreSQL which is close to Spark SQL and this query does execute in Postgre 12.3
# MAGIC 
# MAGIC WITH RECURSIVE distinct_records AS (
# MAGIC   SELECT DISTINCT * 
# MAGIC   FROM pre_collapse
# MAGIC )
# MAGIC ,
# MAGIC 
# MAGIC ranked_by_begindt AS (
# MAGIC   SELECT memnum
# MAGIC      , begindt
# MAGIC      , enddt
# MAGIC      , ROW_NUMBER() OVER ( PARTITION BY memnum ORDER BY begindt ASC) AS memnum_rk
# MAGIC   FROM distinct_records
# MAGIC )
# MAGIC ,
# MAGIC 
# MAGIC recursive_result AS (
# MAGIC   SELECT *
# MAGIC       , 0 AS group_num
# MAGIC       , enddt AS local_maxenddt
# MAGIC   FROM ranked_by_begindt
# MAGIC   WHERE memnum_rk = 1 
# MAGIC   
# MAGIC   UNION 
# MAGIC 
# MAGIC   SELECT c.memnum
# MAGIC      , c.begindt
# MAGIC      , c.enddt
# MAGIC      , c.memnum_rk AS memnum_rk
# MAGIC      , CASE 
# MAGIC          WHEN EXTRACT(DAY FROM c.begindt::timestamp - recur.enddt::timestamp) <= 1 
# MAGIC            OR EXTRACT(DAY FROM c.begindt::timestamp - recur.local_maxenddt::timestamp) <= 1 
# MAGIC              THEN recur.group_num
# MAGIC          ELSE recur.group_num + 1
# MAGIC        END AS group_num
# MAGIC      , CASE 
# MAGIC          WHEN ( EXTRACT(DAY FROM c.begindt::timestamp - recur.enddt::timestamp) <= 1 
# MAGIC            OR EXTRACT(DAY FROM c.begindt::timestamp - recur.local_maxenddt::timestamp) <= 1 ) 
# MAGIC              AND EXTRACT(DAY FROM recur.local_maxenddt::timestamp - recur.enddt::timestamp) > 0 
# MAGIC                THEN recur.local_maxenddt
# MAGIC          WHEN ( EXTRACT(DAY FROM c.begindt::timestamp - recur.enddt::timestamp) <= 1 
# MAGIC            OR EXTRACT(DAY FROM c.begindt::timestamp - recur.local_maxenddt::timestamp) <= 1 ) 
# MAGIC              AND EXTRACT(DAY FROM recur.enddt::timestamp - recur.local_maxenddt::timestamp) <= 0 
# MAGIC                THEN c.enddt
# MAGIC          WHEN ( EXTRACT(DAY FROM c.begindt::timestamp - recur.enddt::timestamp) > 1 
# MAGIC            AND EXTRACT(DAY FROM c.begindt::timestamp - recur.local_maxenddt::timestamp) > 1 )
# MAGIC              THEN c.enddt
# MAGIC        END AS local_maxenddt
# MAGIC   FROM recursive_result recur 
# MAGIC   JOIN ranked_by_begindt c 
# MAGIC     ON c.memnum_rk = recur.memnum_rk + 1
# MAGIC       AND c.memnum = recur.memnum
# MAGIC )
# MAGIC 
# MAGIC SELECT memnum
# MAGIC   , MIN(begindt) AS eligeff_coll
# MAGIC   , MAX(enddt) AS eligend_coll
# MAGIC FROM recursive_result 
# MAGIC GROUP BY memnum 
# MAGIC   , group_num 
# MAGIC ORDER BY memnum ASC
# MAGIC   , eligeff_coll ASC
