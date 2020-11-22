# Databricks notebook source
# MAGIC %md
# MAGIC Give a Stock TICKER Symbol that will get historical information 
# MAGIC Add the current info as another row to the historical data set 
# MAGIC 
# MAGIC - date
# MAGIC - open 
# MAGIC - high 
# MAGIC - low
# MAGIC - close (it wont be actual close, it will be time before close so that we can either buy or sell for next day)
# MAGIC - volume

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val s = dbutils.notebook.getContext().apiToken.get

# COMMAND ----------

import collections
import copy
import json
import re
import requests
import time
from datetime import datetime

from pyspark.sql.types import LongType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import StringType
from pyspark.sql.types import MapType
from pyspark.sql.types import DateType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import IntegerType
from pyspark.sql import Row
from itertools import *
from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.classification import LogisticRegression

# COMMAND ----------

ticker_symbol = "AAPL"
legal_trash = """"MacroTrends Data Download"
"{ticker_symbol} - Historical Price and Volume Data"
"Note: Historical prices are adjusted for stock splits."

"Disclaimer and Terms of Use: Historical stock data is provided 'as is' and solely for informational purposes, not for trading purposes or advice."
"MacroTrends LLC expressly disclaims the accuracy, adequacy, or completeness of any data and shall not be liable for any errors, omissions or other defects in, "
"delays or interruptions in such data, or for any actions taken in reliance thereon.  Neither MacroTrends LLC nor any of our information providers will be liable"
"for any damages relating to your use of the data provided."


"ATTRIBUTION: Proper attribution requires clear indication of the data source as ""www.macrotrends.net""."
"A ""dofollow"" backlink to the originating page is also required if the data is displayed on a web page."


""".format(ticker_symbol=ticker_symbol)
header_cols = "date,open,high,low,close,volume"
response = requests.get("http://download.macrotrends.net/assets/php/stock_data_export.php?t={ticker_symbol}".format(ticker_symbol=ticker_symbol))

# COMMAND ----------

schema = StructType([StructField("date", StringType(), True), StructField("open", DecimalType(10,5), True), StructField("high", DecimalType(10,5), True), 
                     StructField("low", DecimalType(10,5), True), StructField("close", DecimalType(10,5), True), StructField("volume", IntegerType(), True)])
response_sans_legal = response.text.replace(legal_trash, "")


R = Row('date', 'open','high', 'low','close', 'volume')
stock_list = []
for line in response_sans_legal.splitlines()[1:]:
  ls = line.split(",")
  stock_list.append(R(ls[0], ls[1], ls[2],ls[3],ls[4],ls[5]))

  
df = spark.createDataFrame(stock_list)
typed_df = df.selectExpr("to_date(date) AS date", "cast(open AS Decimal(11,5)) AS open", "cast(high AS Decimal(11,5)) AS high", "cast(low AS Decimal(11,5)) AS low", 
                      "cast(close AS Decimal(11,5)) AS close", "cast(volume AS Integer) AS volume")

# pandas creation - not needed
# import pandas as pd
# headers = ['date', 'open','high', 'low','close', 'volume']

# pdf = pd.DataFrame.from_dict(dict(zip(headers, stock_list)))

# COMMAND ----------

display(typed_df.orderBy(desc("date")))

# COMMAND ----------

date_ranked_df = typed_df.select("*", row_number().over(Window.orderBy(col("Date").desc())).alias("Date_Rank"))
df1 = date_ranked_df.alias("df1")
df2 = date_ranked_df.alias("df2")

with_1_day_difference_df = df1.alias("dfDayAhead").join(
  df2.select(
    col("Open").alias("Open1DayAgo")
    , col("High").alias("High1DayAgo")
    , col("Low").alias("Low1DayAgo")
    , col("Close").alias("Close1DayAgo")
    , col("Volume").alias("Volume1DayAgo")
    , col("Date_Rank").alias("DateRank1DayBehind")
    , col("Date").alias("Date1DayBehind"))
    , col("DateRank1DayBehind") == (col("dfDayAhead.Date_Rank") + 1), "inner")

with_2_day_difference_df = with_1_day_difference_df.alias("dfDayAhead").join(
  df2.select(
    col("Open").alias("Open2DayAgo")
   , col("High").alias("High2DayAgo")
   , col("Low").alias("Low2DayAgo")
   , col("Close").alias("Close2DayAgo")
   , col("Volume").alias("Volume2DayAgo")
   , col("Date_Rank").alias("DateRank2DayBehind")
   , col("Date").alias("Date2DayBehind"))
   , col("DateRank2DayBehind") == (col("dfDayAhead.Date_Rank") + 2), "inner")

with_2_day_difference_df.createOrReplaceTempView("stock_last_10_days")

with_3_day_difference_df = with_2_day_difference_df.alias("dfDayAhead").join(
  df2.select(
    col("Open").alias("Open3DayAgo")
   , col("High").alias("High3DayAgo")
   , col("Low").alias("Low3DayAgo")
   , col("Close").alias("Close3DayAgo")
   , col("Volume").alias("Volume3DayAgo")
   , col("Date_Rank").alias("DateRank3DayBehind")
   , col("Date").alias("Date3DayBehind"))
  , col("DateRank3DayBehind") == (col("dfDayAhead.Date_Rank") + 3), "inner")

with_4_day_difference_df = with_3_day_difference_df.alias("dfDayAhead").join(
  df2.select(
    col("Open").alias("Open4DayAgo")
   , col("High").alias("High4DayAgo")
   , col("Low").alias("Low4DayAgo")
   , col("Close").alias("Close4DayAgo")
   , col("Volume").alias("Volume4DayAgo")
   , col("Date_Rank").alias("DateRank4DayBehind")
   , col("Date").alias("Date4DayBehind"))
  , col("DateRank4DayBehind") == (col("dfDayAhead.Date_Rank") + 4), "inner")

with_5_day_difference_df = with_4_day_difference_df.alias("dfDayAhead").join(
  df2.select(
    col("Open").alias("Open5DayAgo")
   , col("High").alias("High5DayAgo")
   , col("Low").alias("Low5DayAgo")
   , col("Close").alias("Close5DayAgo")
   , col("Volume").alias("Volume5DayAgo")
   , col("Date_Rank").alias("DateRank5DayBehind")
   , col("Date").alias("Date5DayBehind"))
   , col("DateRank5DayBehind") == (col("dfDayAhead.Date_Rank") + 5), "inner")

with_6_day_difference_df = with_5_day_difference_df.alias("dfDayAhead").join(
  df2.select(
    col("Open").alias("Open6DayAgo")
   , col("High").alias("High6DayAgo")
   , col("Low").alias("Low6DayAgo")
   , col("Close").alias("Close6DayAgo")
   , col("Volume").alias("Volume6DayAgo")
   , col("Date_Rank").alias("DateRank6DayBehind")
   , col("Date").alias("Date6DayBehind"))
   , col("DateRank6DayBehind") == (col("dfDayAhead.Date_Rank") + 6), "inner")

with_7_day_difference_df = with_6_day_difference_df.alias("dfDayAhead").join(
  df2.select(
     col("Open").alias("Open7DayAgo")
   , col("High").alias("High7DayAgo")
   , col("Low").alias("Low7DayAgo")
   , col("Close").alias("Close7DayAgo")
   , col("Volume").alias("Volume7DayAgo")
   , col("Date_Rank").alias("DateRank7DayBehind")
   , col("Date").alias("Date7DayBehind"))
  , col("DateRank7DayBehind") == (col("dfDayAhead.Date_Rank") + 7), "inner")

with_8_day_difference_df = with_7_day_difference_df.alias("dfDayAhead").join(
  df2.select(col("Open").alias("Open8DayAgo")
   , col("High").alias("High8DayAgo")
   , col("Low").alias("Low8DayAgo")
   , col("Close").alias("Close8DayAgo")
   , col("Volume").alias("Volume8DayAgo")
   , col("Date_Rank").alias("DateRank8DayBehind")
   , col("Date").alias("Date8DayBehind"))
   , col("DateRank8DayBehind") == (col("dfDayAhead.Date_Rank") + 8), "inner")

with_9_day_difference_df = with_8_day_difference_df.alias("dfDayAhead").join(
  df2.select(col("Open").alias("Open9DayAgo")
  , col("High").alias("High9DayAgo")
  , col("Low").alias("Low9DayAgo")
  , col("Close").alias("Close9DayAgo")
  , col("Volume").alias("Volume9DayAgo")
  , col("Date_Rank").alias("DateRank9DayBehind")
  , col("Date").alias("Date9DayBehind"))
  , col("DateRank9DayBehind") == (col("dfDayAhead.Date_Rank") + 9), "inner")

with_10_day_difference_df = with_9_day_difference_df.alias("dfDayAhead").join(
  df2.select(col("Open").alias("Open10DayAgo")
  , col("High").alias("High10DayAgo")
  , col("Low").alias("Low10DayAgo")
  , col("Close").alias("Close10DayAgo")
  , col("Volume").alias("Volume10DayAgo")
  , col("Date_Rank").alias("DateRank10DayBehind")
  , col("Date").alias("Date10DayBehind"))
  , col("DateRank10DayBehind") == (col("dfDayAhead.Date_Rank") + 10), "inner")

with_10_day_difference_df.createOrReplaceTempView("stock_last_10_days")

features_df = spark.sql(""" 
WITH cte0 AS
  (
    SELECT *
       , (Close - Open) AS CloseOpenDiffForDay
       , LAG(
            CASE 
              /*
                Buy if the next day the stock will raise 1.5 percent or more
              */
              WHEN ( ((Close - Close1DayAgo) / Close1DayAgo) * 100 ) > 1.5 THEN 1 
              ELSE 0 
            END
            , 1) OVER (ORDER BY Date_Rank) AS BuyBeforeCloseTodaySoYouMakeMoneyTomorrow
       , LAG( ( (Close - Close1DayAgo) / Close1DayAgo) ) OVER (ORDER BY Date_Rank) AS NextDayClose            
       , High - High1DayAgo AS DollarDiffHighANDHigh1DayAgo
       , High - High2DayAgo AS DollarDiffHighANDHigh2DayAgo
       , High - High3DayAgo AS DollarDiffHighANDHigh3DayAgo
       , High - High4DayAgo AS DollarDiffHighANDHigh4DayAgo
       , High - High5DayAgo AS DollarDiffHighANDHigh5DayAgo
       , High - High10DayAgo AS DollarDiffHighANDHigh10DayAgo
       
       , High1DayAgo - High2DayAgo AS DollarDiffHigh1DayAgoANDHigh2DayAgo
       , High1DayAgo - High3DayAgo AS DollarDiffHigh1DayAgoANDHigh3DayAgo
       , High1DayAgo - High4DayAgo AS DollarDiffHigh1DayAgoANDHigh4DayAgo
       , High1DayAgo - High5DayAgo AS DollarDiffHigh1DayAgoANDHigh5DayAgo
       , High1DayAgo - High10DayAgo AS DollarDiffHigh1DayAgoANDHigh10DayAgo      
       
       , High2DayAgo - High3DayAgo AS DollarDiffHigh2DayAgoANDHigh3DayAgo
       , High3DayAgo - High4DayAgo AS DollarDiffHigh3DayAgoANDHigh4DayAgo
       , High4DayAgo - High5DayAgo AS DollarDiffHigh4DayAgoANDHigh5DayAgo
       , High5DayAgo - High6DayAgo AS DollarDiffHigh5DayAgoANDHigh6DayAgo
       , High6DayAgo - High7DayAgo AS DollarDiffHigh6DayAgoANDHigh7DayAgo
       , High7DayAgo - High8DayAgo AS DollarDiffHigh7DayAgoANDHigh8DayAgo
       , High8DayAgo - High9DayAgo AS DollarDiffHigh8DayAgoANDHigh9DayAgo
       , High9DayAgo - High10DayAgo AS DollarDiffHigh9DayAgoANDHigh10DayAgo
       
       , (Close - Close1DayAgo) / Close1DayAgo AS DiffCloseANDClose1DayAgoOVERClose1DayAgo
       , (Close - Close2DayAgo) / Close2DayAgo AS DiffCloseANDClose2DayAgoOVERClose2DayAgo
       , (Close - Close3DayAgo) / Close3DayAgo AS DiffCloseANDClose3DayAgoOVERClose3DayAgo
       , (Close - Close4DayAgo) / Close4DayAgo AS DiffCloseANDClose4DayAgoOVERClose4DayAgo
       , (Close - Close5DayAgo) / Close5DayAgo AS DiffCloseANDClose5DayAgoOVERClose5DayAgo 
       , (Close - Close10DayAgo) / Close10DayAgo AS DiffCloseANDClose10DayAgoOVERClose10DayAgo 
  
       , ((Close - Close1DayAgo) / Close1DayAgo ) - (Close - (LAG(Close, 10) OVER (ORDER BY Date_Rank DESC))) / (LAG(Close, 10) OVER (ORDER BY Date_Rank DESC)) AS NetLast1DaysMINUSNetLast10Days
       , ((Close - Close1DayAgo) / Close1DayAgo ) - (Close - (LAG(Close, 20) OVER (ORDER BY Date_Rank DESC))) / (LAG(Close, 20) OVER (ORDER BY Date_Rank DESC)) AS NetLast1DaysMINUSNetLast20Days
       , ((Close - Close1DayAgo) / Close1DayAgo ) - (Close - (LAG(Close, 30) OVER (ORDER BY Date_Rank DESC))) / (LAG(Close, 30) OVER (ORDER BY Date_Rank DESC)) AS NetLast1DaysMINUSNetLast30Days 
       , ((Close - Close2DayAgo) / Close2DayAgo ) - (Close - (LAG(Close, 10) OVER (ORDER BY Date_Rank DESC))) / (LAG(Close, 10) OVER (ORDER BY Date_Rank DESC)) AS NetLast2DaysMINUSNetLast10Days
       , ((Close - Close2DayAgo) / Close2DayAgo ) - (Close - (LAG(Close, 20) OVER (ORDER BY Date_Rank DESC))) / (LAG(Close, 20) OVER (ORDER BY Date_Rank DESC)) AS NetLast2DaysMINUSNetLast20Days
       , ((Close - Close2DayAgo) / Close2DayAgo ) - (Close - (LAG(Close, 30) OVER (ORDER BY Date_Rank DESC))) / (LAG(Close, 30) OVER (ORDER BY Date_Rank DESC)) AS NetLast2DaysMINUSNetLast30Days
       , ((Close - Close3DayAgo) / Close3DayAgo ) - (Close - (LAG(Close, 10) OVER (ORDER BY Date_Rank DESC))) / (LAG(Close, 10) OVER (ORDER BY Date_Rank DESC)) AS NetLast3DaysMINUSNetLast10Days
       , ((Close - Close3DayAgo) / Close3DayAgo ) - (Close - (LAG(Close, 20) OVER (ORDER BY Date_Rank DESC))) / (LAG(Close, 20) OVER (ORDER BY Date_Rank DESC)) AS NetLast3DaysMINUSNetLast20Days
       , ((Close - Close3DayAgo) / Close3DayAgo ) - (Close - (LAG(Close, 30) OVER (ORDER BY Date_Rank DESC))) / (LAG(Close, 30) OVER (ORDER BY Date_Rank DESC)) AS NetLast3DaysMINUSNetLast30Days
 
 
       , (((Close - Close1DayAgo) / Close1DayAgo ) - ((Close1Dayago - Close5DayAgo) / Close5DayAgo) ) AS NetLast1DaysMINUSNetPrevious4Days
       , (((Close - Close1DayAgo) / Close1DayAgo ) - ((Close1Dayago - Close6DayAgo) / Close6DayAgo) ) AS NetLast1DaysMINUSNetPrevious5Days
       , (((Close - Close1DayAgo) / Close1DayAgo ) - ((Close1Dayago - Close7DayAgo) / Close7DayAgo) ) AS NetLast1DaysMINUSNetPrevious6Days
       , (((Close - Close2DayAgo) / Close2DayAgo ) - ((Close2Dayago - Close6DayAgo) / Close6DayAgo) ) AS NetLast2DaysMINUSNetPrevious4Days
       , (((Close - Close2DayAgo) / Close2DayAgo ) - ((Close2Dayago - Close7DayAgo) / Close7DayAgo) ) AS NetLast2DaysMINUSNetPrevious5Days
       , (((Close - Close2DayAgo) / Close2DayAgo ) - ((Close2Dayago - Close8DayAgo) / Close8DayAgo) ) AS NetLast2DaysMINUSNetPrevious6Days
       , (((Close - Close3DayAgo) / Close3DayAgo ) - ((Close3Dayago - Close7DayAgo) / Close7DayAgo) ) AS NetLast3DaysMINUSNetPrevious4Days
       , (((Close - Close3DayAgo) / Close3DayAgo ) - ((Close3Dayago - Close8DayAgo) / Close8DayAgo) ) AS NetLast3DaysMINUSNetPrevious5Days
       , (((Close - Close3DayAgo) / Close3DayAgo ) - ((Close3Dayago - Close9DayAgo) / Close9DayAgo) ) AS NetLast3DaysMINUSNetPrevious6Days
       
       , (((Close - Close5DayAgo) / Close5DayAgo ) - ((Close5Dayago - Close10DayAgo) / Close10DayAgo) ) AS NetLast5DaysMINUSNetPrevious5Days
       
       , (((Close - Close5DayAgo) / Close5DayAgo ) - ((Close - Close10DayAgo) / Close10DayAgo) ) AS NetLast5DaysMINUSNetLast10Days
       
       /*, ((Close - Close5DayAgo) / Close5DayAgo ) - ( Close - (LAG(Close, 20) OVER (ORDER BY Date_Rank DESC)) / (LAG(Close, 20) OVER (ORDER BY Date_Rank))) AS NetLast5DaysMINUSNetLast20Days*/
       , ((Close - Close5DayAgo) / Close5DayAgo ) - (Close - (LAG(Close, 20) OVER (ORDER BY Date_Rank DESC))) / (LAG(Close, 20) OVER (ORDER BY Date_Rank DESC)) AS NetLast5DaysMINUSNetLast20Days
       
       
       
       , (Close1DayAgo - Close2DayAgo) / Close2DayAgo AS DiffClose1DayAgoANDClose2DayAgoOVERClose2DayAgo
       , (Close1DayAgo - Close3DayAgo) / Close3DayAgo AS DiffClose1DayAgoANDClose3DayAgoOVERClose3DayAgo
       , (Close1DayAgo - Close4DayAgo) / Close4DayAgo AS DiffClose1DayAgoANDClose4DayAgoOVERClose4DayAgo
       , (Close1DayAgo - Close5DayAgo) / Close5DayAgo AS DiffClose1DayAgoANDClose5DayAgoOVERClose5DayAgo 
       , (Close1DayAgo - Close10DayAgo) / Close10DayAgo AS DiffClose1DayAgoANDClose10DayAgoOVERClose10DayAgo
       , (Close2DayAgo - Close3DayAgo) / Close3DayAgo AS DiffClose2DayAgoANDClose3DayAgoOVERClose3DayAgo 
       , (Close3DayAgo - Close4DayAgo) / Close4DayAgo AS DiffClose3DayAgoANDClose4DayAgoOVERClose4DayAgo
       , (Close4DayAgo - Close5DayAgo) / Close5DayAgo AS DiffClose4DayAgoANDClose5DayAgoOVERClose5DayAgo 
       , (Close5DayAgo - Close6DayAgo) / Close6DayAgo AS DiffClose5DayAgoANDClose6DayAgoOVERClose6DayAgo 
       , (Close6DayAgo - Close7DayAgo) / Close7DayAgo AS DiffClose6DayAgoANDClose7DayAgoOVERClose7DayAgo 
       , (Close7DayAgo - Close8DayAgo) / Close8DayAgo AS DiffClose7DayAgoANDClose8DayAgoOVERClose8DayAgo 
       , (Close8DayAgo - Close9DayAgo) / Close9DayAgo AS DiffClose8DayAgoANDClose9DayAgoOVERClose9DayAgo 
       , (Close9DayAgo - Close10DayAgo) / Close10DayAgo AS DiffClose9DayAgoANDClose10DayAgoOVERClose10DayAgo
       
       , (High1DayAgo - Low1DayAgo) / (High2DayAgo - Low2DayAgo) AS RangeDayAgoOVERRange2DayAgo 
       , (High1DayAgo - Low1DayAgo) / (High3DayAgo - Low3DayAgo) AS Range1DayAgoOVERRange3DayAgo
       , (High1DayAgo - Low1DayAgo) / (High4DayAgo - Low4DayAgo) AS Range1DayAgoOVERRange4DayAgo 
       , (High1DayAgo - Low1DayAgo) / (High5DayAgo - Low5DayAgo) AS Range1DayAgoOVERRange5DayAgo 
       , (High1DayAgo - Low1DayAgo) / (High10DayAgo - Low10DayAgo) AS Range1DayAgoOVERRange10DayAgo 
       
       , (High2DayAgo - Low2DayAgo) / (High3DayAgo - Low3DayAgo) AS Range2DayAgoOVERRange3DayAgo 
       , (High3DayAgo - Low3DayAgo) / (High4DayAgo - Low4DayAgo) AS Range3DayAgoOVERRange4DayAgo 
       , (High4DayAgo - Low4DayAgo) / (High5DayAgo - Low5DayAgo) AS Range4DayAgoOVERRange5DayAgo 
       , (High5DayAgo - Low5DayAgo) / (High6DayAgo - Low6DayAgo) AS Range5DayAgoOVERRange6DayAgo
       , (High6DayAgo - Low6DayAgo) / (High7DayAgo - Low7DayAgo) AS Range6DayAgoOVERRange7DayAgo
       , (High7DayAgo - Low7DayAgo) / (High8DayAgo - Low8DayAgo) AS Range7DayAgoOVERRange8DayAgo 
       , (High8DayAgo - Low8DayAgo) / (High9DayAgo - Low9DayAgo) AS Range8DayAgoOVERRange9DayAgo 
       , (High9DayAgo - Low9DayAgo) / (High10DayAgo - Low10DayAgo) AS Range9DayAgoOVERRange10DayAgo

       , (High10DayAgo - High7DayAgo) / (High3DayAgo - High1DayAgo) AS DiffHigh10DayAgoAND7DayAgoOVERDiffHigh3DayAgoAnd1DayAgo
       , (High9DayAgo - High6DayAgo) / (High3DayAgo - High1DayAgo) AS DiffHigh9DayAgoAND6DayAgoOVERDiffHigh3DayAgoAnd1DayAgo 
       , (High8DayAgo - High5DayAgo) / (High3DayAgo - High1DayAgo) AS DiffHigh8DayAgoAND5DayAgoOVERDiffHigh3DayAgoAnd1DayAgo  
       , (High7DayAgo - High4DayAgo) / (High3DayAgo - High1DayAgo) AS DiffHigh7DayAgoAND4DayAgoOVERDiffHigh3DayAgoAnd1DayAgo 
    FROM stock_last_10_days
  )
  , 
  cte1 AS 
  (
    SELECT CASE WHEN close - close1DayAgo >= 0 THEN 1 ELSE 0 END AS PositiveDay
      , CASE WHEN close - close1DayAgo < 0 THEN 1 ELSE 0 END AS NegativeDay
      , *
    FROM cte0
  )
  , cte2 AS
  (
    SELECT SUM(CASE WHEN ( LAG(PositiveDay, 1) OVER (ORDER BY Date_Rank) = 0)  AND PositiveDay = 1 THEN 1 ELSE 0 END) OVER (ORDER BY Date_Rank) PositiveDayGroupRaw
      , SUM(CASE WHEN ( LAG(NegativeDay, 1) OVER (ORDER BY Date_Rank) = 0)  AND NegativeDay = 1 THEN 1 ELSE 0 END) OVER (ORDER BY Date_Rank) NegativeDayGroupRaw
      , *
    FROM cte1
  )
  , 
  cte3 AS 
  (
    SELECT CASE WHEN PositiveDay = 0 THEN -1 ELSE PositiveDayGroupRaw END AS PositiveDayGroup
      , CASE WHEN NegativeDay = 0 THEN -1 ELSE NegativeDayGroupRaw END AS NegativeDayGroup
      , *
    FROM cte2
  )
  , 
  cte4 AS 
  (
    SELECT CASE WHEN PositiveDay = 1 THEN RANK() OVER (PARTITION BY PositiveDayGroup ORDER BY Date_Rank DESC) ELSE 0 END AS PositiveDayInARow
      , CASE WHEN NegativeDay = 1 THEN RANK() OVER (PARTITION BY NegativeDayGroup ORDER BY Date_Rank DESC) ELSE 0 END * -1 AS NegativeDayInARow
      , * 
    FROM cte3
  )
 
 
  SELECT CAST(
           (
             CASE 
               WHEN PositiveDay = 1 THEN PositiveDayInARow
               ELSE NegativeDayInARow
             END
           )
           AS NUMERIC(10,4)
           ) AS Trend
     , CASE 
         WHEN PositiveDay = 1 THEN PositiveDayInARow
         ELSE 0
       END AS PositiveTrend
     , CASE 
         WHEN NegativeDay = 1 THEN NegativeDayInARow
         ELSE 0
       END AS NegativeTrend
     , *
  FROM cte4
  ORDER BY Date_Rank ASC
 """)


# COMMAND ----------

"""
Ignored Columns 
Sometimes the basic columns or the date are useful to have in the final dataframe.
They are deperated for easier developement (because it's quicker to comment them out)
"""
label_columns_used = ['BuyBeforeCloseTodaySoYouMakeMoneyTomorrow']

date_column = ['date']
basic_columns = ['open', 'high', 'low', 'volume', 'close',]
label_columns = ['label', 'BuyBeforeCloseTodaySoYouMakeMoneyTomorrow', 'NextDayClose']
ignored_columns = ( 
#   date_column + 
  list(set(label_columns) - set(label_columns_used)) +
  basic_columns + 
  [
    'Adj Close', 'Date_Rank', 'DateRank1DayBehind', 'Date1DayBehind', 'DateRank2DayBehind', 'Date2DayBehind', 'Date3DayBehind', 'DateRank3DayBehind', 
    'DateRank4DayBehind', 'Date4DayBehind', 'Date5DayBehind', 'DateRank5DayBehind', 'DateRank6DayBehind', 'Date6DayBehind', 'Date7DayBehind', 'DateRank7DayBehind', 
    'DateRank8DayBehind', 'Date8DayBehind', 'Date9DayBehind', 'DateRank9DayBehind', 'DateRank10DayBehind','Date10DayBehind','HLAbsoluteVolatility', 'OCVolatility',
    'Open1DayAgo', 'High1DayAgo', 'Low1DayAgo', 'Close1DayAgo', 'Open2DayAgo', 'High2DayAgo', 'Low2DayAgo', 'Close2DayAgo', 'Open3DayAgo', 'High3DayAgo', 'Low3DayAgo', 
    'Close3DayAgo', 'Open4DayAgo', 'High4DayAgo', 'Low4DayAgo', 'Close4DayAgo', 'Open5DayAgo', 'High5DayAgo', 'Low5DayAgo', 'Close5DayAgo', 'Open6DayAgo', 'High6DayAgo', 
    'Low6DayAgo', 'Close6DayAgo', 'Open7DayAgo', 'High7DayAgo', 'Low7DayAgo', 'Close7DayAgo', 'Open8DayAgo', 'High8DayAgo', 'Low8DayAgo', 'Close8DayAgo', 'Open9DayAgo', 
    'High9DayAgo', 'Low9DayAgo', 'Close9DayAgo', 'Open10DayAgo', 'High10DayAgo', 'Low10DayAgo', 'Close10DayAgo', 'BiRegrLabel', 'PositiveDayInARow', 'NegativeDayInARow', 
    'PositiveDayGroup', 'NegativeDayGroup', 'PositiveDayGroupRaw', 'NegativeDayGroupRaw', 'NegativeDay', 'Volume1DayAgo', 'Volume2DayAgo', 'Volume3DayAgo', 
    'Volume4DayAgo', 'Volume5DayAgo', 'Volume6DayAgo', 'Volume7DayAgo', 'Volume8DayAgo', 'Volume9DayAgo', 'Volume10DayAgo'
  ]
)

"""
Declare which columns will be used for features. 
We use the ignored_columns list above to exclude ones that are used just for feature creation

For some development might need to add columsn in like the label and date, which will need to 
be taken out later

For quick testing, just use subset of columns below
"""
# feature_cols = [
#   'Trend', 
#   'date',
#   'BuyBeforeCloseTodaySoYouMakeMoneyTomorrow', 
#   'NetLast5DaysMINUSNetPrevious5Days', 
#   'NetLast5DaysMINUSNetLast10Days', 
#   'NetLast5DaysMINUSNetLast20Days', 
#  ]

feature_cols = [c for c in features_df.columns if c not in ignored_columns] 

"""
Delete the most recent row because there is no label 
Rename the label_column to "label"

"""
most_recent_exclude_expr = "Date_Rank <> 1"

#features_trimmed_df = features_df.filter("Date_Rank <> 1").select(*feature_cols).withColumnRenamed("BuyBeforeCloseTodaySoYouMakeMoneyTomorrow", "label")
features_trimmed_df = (
  features_df
    .filter(most_recent_exclude_expr)
    .select(*feature_cols)
    .withColumnRenamed(label_used, "label")
)
                            
#take out columns added for description, these columns are just for the features Vector                              
feature_trimmed_cols = [c for c in features_trimmed_df.columns if c not in ['label', 'date']] 

#display(features_trimmed_df)

#write to temp table
features_trimmed_df.createOrReplaceTempView("stock_features")

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT COUNT(*) FROM stock_features WHERE label = 1
# MAGIC SELECT * FROM stock_features WHERE label = 1

# COMMAND ----------

# MAGIC %scala 
# MAGIC import org.apache.spark.ml.regression.GeneralizedLinearRegression
# MAGIC import org.apache.spark.ml.feature.PolynomialExpansion
# MAGIC import org.apache.spark.ml.feature.VectorAssembler
# MAGIC import org.apache.spark.ml.feature.PCA
# MAGIC import org.apache.spark.mllib.linalg.Vectors
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC val vudt = udf((dv: org.apache.spark.ml.linalg.DenseVector) => 
# MAGIC                {
# MAGIC                  for (c <- dv.getClass.getFields) yield c.getName
# MAGIC                }.mkString(", ")
# MAGIC )
# MAGIC 
# MAGIC val featuresTrimmedDF = spark.read.table("stock_features")
# MAGIC //val featuresTrimmedDF = spark.sql("SELECT * FROM stock_features WHERE date > '2010-01-01'")
# MAGIC 
# MAGIC val featureCols = featuresTrimmedDF.columns.filter(c => !(Array("label", "date").contains(c)))
# MAGIC 
# MAGIC // val featuresAndLabelDf = logRegAssembler.setHandleInvalid("skip").transform(featuresTrimmedDF)
# MAGIC 
# MAGIC val featuresDF = new VectorAssembler()
# MAGIC   .setInputCols(featureCols)
# MAGIC   .setOutputCol("constructedFeatures")
# MAGIC   .setHandleInvalid("skip")
# MAGIC   .transform(featuresTrimmedDF)
# MAGIC 
# MAGIC val pcaFeaturesDF = new PCA()
# MAGIC   .setInputCol("constructedFeatures")
# MAGIC   .setOutputCol("pcaFeatures")
# MAGIC   .setK(3)
# MAGIC   .fit(featuresDF)
# MAGIC   .transform(featuresDF)
# MAGIC 
# MAGIC val polyExpansionDF = new PolynomialExpansion()
# MAGIC   .setInputCol("constructedFeatures")
# MAGIC   .setOutputCol("polyFeatures")
# MAGIC   .setDegree(2)
# MAGIC   .transform(pcaFeaturesDF)
# MAGIC 
# MAGIC val colArray = Array(
# MAGIC //   "constructedFeatures" 
# MAGIC    "pcaFeatures"
# MAGIC //   , "polyFeatures"
# MAGIC ) ++ featureCols
# MAGIC 
# MAGIC val multiVectorAssembler = new VectorAssembler()
# MAGIC     .setInputCols(colArray)
# MAGIC     .setOutputCol("features")
# MAGIC 
# MAGIC val exportDF = multiVectorAssembler.setHandleInvalid("skip").transform(polyExpansionDF).select("label", "features")
# MAGIC 
# MAGIC println(exportDF.printSchema)
# MAGIC exportDF.createOrReplaceTempView("pca")
# MAGIC 
# MAGIC // display(pca.transform(featuresAndLabelDf))
# MAGIC // val featuresAndLabelDf = assembler.setHandleInvalid("skip").transform(featuresTrimmedDF)
# MAGIC 
# MAGIC val glr = new GeneralizedLinearRegression()
# MAGIC   .setFamily("gaussian")
# MAGIC   .setLink("identity")
# MAGIC   .setMaxIter(10)
# MAGIC   .setRegParam(0.3)
# MAGIC 
# MAGIC val model = glr.fit(exportDF)
# MAGIC 
# MAGIC // Print the coefficients and intercept for generalized linear regression model
# MAGIC println(s"Coefficients: ${model.coefficients}")
# MAGIC println(s"Intercept: ${model.intercept}")
# MAGIC 
# MAGIC // Summarize the model over the training set and print out some metrics
# MAGIC val summary = model.summary
# MAGIC println(s"Count of Rows: ${featuresTrimmedDF.count}")
# MAGIC println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
# MAGIC println(s"T Values: ${summary.tValues.mkString(",")}")
# MAGIC println(s"P Values: ${summary.pValues.mkString(",")}")
# MAGIC println(s"Dispersion: ${summary.dispersion}")
# MAGIC println(s"Null Deviance: ${summary.nullDeviance}")
# MAGIC println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
# MAGIC println(s"Deviance: ${summary.deviance}")
# MAGIC println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
# MAGIC println(s"AIC: ${summary.aic}")
# MAGIC println("Deviance Residuals: ")
# MAGIC summary.residuals().show()

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.labs.automl.executor.config.ConfigurationGenerator
# MAGIC import com.databricks.labs.automl.executor.FamilyRunner
# MAGIC 
# MAGIC val data = spark.table("stock_features")
# MAGIC 
# MAGIC val overrides = Map("labelCol" -> "label",
# MAGIC "mlFlowExperimentName" -> "/Users/michael.artz@databricks.com/stocks_test",
# MAGIC "mlFlowTrackingURI" -> "https://cust-success.cloud.databricks.com",
# MAGIC "mlFlowAPIToken" -> dbutils.notebook.getContext().apiToken.get,
# MAGIC "mlFlowModelSaveDirectory" -> "/ml2/FirstAutoMLRun/",
# MAGIC "inferenceConfigSaveLocation" -> "ml2/FirstAutoMLRun/inference"
# MAGIC )
# MAGIC 
# MAGIC //val randomForestConfig = ConfigurationGenerator.generateConfigFromMap("RandomForest", "classifier", overrides)
# MAGIC val gbtConfig = ConfigurationGenerator.generateConfigFromMap("GBT", "classifier", overrides)
# MAGIC //val logConfig = ConfigurationGenerator.generateConfigFromMap("LogisticRegression", "classifier", overrides)
# MAGIC 
# MAGIC //val runner = FamilyRunner(data, Array(randomForestConfig, gbtConfig, logConfig)).execute()
# MAGIC val runner = FamilyRunner(data, Array(gbtConfig)).execute()
