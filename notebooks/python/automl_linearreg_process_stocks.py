# Databricks notebook source
# MAGIC %scala
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

date_ranked_df = typed_df.select("*", row_number().over(Window.orderBy(col("Date").desc())).alias("Date_Rank"))
df1 = date_ranked_df.alias("df1")
df2 = date_ranked_df.alias("df2")

with_1_day_difference_df = df1.alias("dfDayAhead").join(
                    df2.select(col("Open").alias("Open1DayAgo")
                  , col("High").alias("High1DayAgo")
                  , col("Low").alias("Low1DayAgo")
                  , col("Close").alias("Close1DayAgo")
                  , col("Volume").alias("Volume1DayAgo")
                  , col("Date_Rank").alias("DateRank1DayBehind")
                  , col("Date").alias("Date1DayBehind"))
            , col("DateRank1DayBehind") == (col("dfDayAhead.Date_Rank") + 1), "inner")

with_2_day_difference_df = with_1_day_difference_df.alias("dfDayAhead").join(
    df2.select(col("Open").alias("Open2DayAgo")
               , col("High").alias("High2DayAgo")
               , col("Low").alias("Low2DayAgo")
               , col("Close").alias("Close2DayAgo")
               , col("Volume").alias("Volume2DayAgo")
               , col("Date_Rank").alias("DateRank2DayBehind")
               , col("Date").alias("Date2DayBehind"))
  , col("DateRank2DayBehind") == (col("dfDayAhead.Date_Rank") + 2), "inner")

with_3_day_difference_df = with_2_day_difference_df.alias("dfDayAhead").join(
  df2.select(col("Open").alias("Open3DayAgo")
             , col("High").alias("High3DayAgo")
             , col("Low").alias("Low3DayAgo")
             , col("Close").alias("Close3DayAgo")
             , col("Volume").alias("Volume3DayAgo")
             ,  col("Date_Rank").alias("DateRank3DayBehind")
             , col("Date").alias("Date3DayBehind"))
  , col("DateRank3DayBehind") == (col("dfDayAhead.Date_Rank") + 3), "inner")

with_4_day_difference_df = with_3_day_difference_df.alias("dfDayAhead").join(
  df2.select(col("Open").alias("Open4DayAgo")
             , col("High").alias("High4DayAgo")
             , col("Low").alias("Low4DayAgo")
             ,  col("Close").alias("Close4DayAgo")
             , col("Volume").alias("Volume4DayAgo")
             ,  col("Date_Rank").alias("DateRank4DayBehind")
             , col("Date").alias("Date4DayBehind"))
  , col("DateRank4DayBehind") == (col("dfDayAhead.Date_Rank") + 4), "inner")

with_5_day_difference_df = with_4_day_difference_df.alias("dfDayAhead").join(
  df2.select(col("Open").alias("Open5DayAgo")
             , col("High").alias("High5DayAgo")
             , col("Low").alias("Low5DayAgo")
             , col("Close").alias("Close5DayAgo")
             , col("Volume").alias("Volume5DayAgo")
             ,  col("Date_Rank").alias("DateRank5DayBehind")
             , col("Date").alias("Date5DayBehind"))
  , col("DateRank5DayBehind") == (col("dfDayAhead.Date_Rank") + 5), "inner")

with_6_day_difference_df = with_5_day_difference_df.alias("dfDayAhead").join(
  df2.select(col("Open").alias("Open6DayAgo")
             , col("High").alias("High6DayAgo")
             , col("Low").alias("Low6DayAgo")
             , col("Close").alias("Close6DayAgo")
             , col("Volume").alias("Volume6DayAgo")
             ,  col("Date_Rank").alias("DateRank6DayBehind")
             , col("Date").alias("Date6DayBehind"))
  , col("DateRank6DayBehind") == (col("dfDayAhead.Date_Rank") + 6), "inner")

with_7_day_difference_df = with_6_day_difference_df.alias("dfDayAhead").join(
  df2.select(col("Open").alias("Open7DayAgo")
             , col("High").alias("High7DayAgo")
             , col("Low").alias("Low7DayAgo")
             , col("Close").alias("Close7DayAgo")
             , col("Volume").alias("Volume7DayAgo")
             ,  col("Date_Rank").alias("DateRank7DayBehind")
             , col("Date").alias("Date7DayBehind"))
  , col("DateRank7DayBehind") == (col("dfDayAhead.Date_Rank") + 7), "inner")

with_8_day_difference_df = with_7_day_difference_df.alias("dfDayAhead").join(
  df2.select(col("Open").alias("Open8DayAgo")
             , col("High").alias("High8DayAgo")
             , col("Low").alias("Low8DayAgo")
             , col("Close").alias("Close8DayAgo")
             , col("Volume").alias("Volume8DayAgo")
             ,  col("Date_Rank").alias("DateRank8DayBehind")
             , col("Date").alias("Date8DayBehind"))
  , col("DateRank8DayBehind") == (col("dfDayAhead.Date_Rank") + 8), "inner")

with_9_day_difference_df = with_8_day_difference_df.alias("dfDayAhead").join(
  df2.select(col("Open").alias("Open9DayAgo")
             , col("High").alias("High9DayAgo")
             , col("Low").alias("Low9DayAgo")
             , col("Close").alias("Close9DayAgo")
             , col("Volume").alias("Volume9DayAgo")
             ,  col("Date_Rank").alias("DateRank9DayBehind")
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
  SELECT *
       , (Open - Close) * -1 AS OCVolatility
       , CASE 
             WHEN ((Open - Close) * -1) > 1 THEN 1 
             ELSE 0 
         END AS BuyFlag
       , High - Low AS HLAbsoluteVolatility
       , High1DayAgo - High2DayAgo AS DiffHigh1DayAgoTOHigh2DayAgo
       , High2DayAgo - High3DayAgo AS DiffHigh2DayAgoFROMHigh3DayAgo
       , High3DayAgo - High4DayAgo AS DiffHigh3DayAgoFROMHigh4DayAgo
       , High4DayAgo - High5DayAgo AS DiffHigh4DayAgoFROMHigh5DayAgo
       , High5DayAgo - High6DayAgo AS DiffHigh5DayAgoFROMHigh6DayAgo
       , High6DayAgo - High7DayAgo AS DiffHigh6DayAgoFROMHigh7DayAgo
       , High7DayAgo - High8DayAgo AS DiffHigh7DayAgoFROMHigh8DayAgo
       , High8DayAgo - High9DayAgo AS DiffHigh8DayAgoFROMHigh9DayAgo
       , High1DayAgo - High4DayAgo AS DiffHigh1DayAgoFROMHigh3DayAgo
       , High1DayAgo - High4DayAgo AS DiffHigh1DayAgoFROMHigh4DayAgo
       , High1DayAgo - High5DayAgo AS DiffHigh1DayAgoFROMHigh5DayAgo
       , High1DayAgo - High10DayAgo AS DiffHigh1DayAgoFROMHigh10DayAgo
       , High9DayAgo - High10DayAgo AS DiffHigh9DayAgoFROMHigh10DayAgo
       , Close1DayAgo - Close2DayAgo AS DiffCloseTodayFROMClose2DayAgo 
       , Close2DayAgo - Close3DayAgo AS DiffClose2DayAgoFROMClose3DayAgo 
       , Close3DayAgo - Close4DayAgo AS DiffClose3DayAgoFROMClose4DayAgo
       , Close4DayAgo - Close5DayAgo AS DiffClose4DayAgoFROMClose5DayAgo 
       , Close5DayAgo - Close6DayAgo AS DiffClose5DayAgoFROMClose6DayAgo 
       , Close6DayAgo - Close7DayAgo AS DiffClose6DayAgoFROMClose7DayAgo 
       , Close7DayAgo - Close8DayAgo AS DiffClose7DayAgoFROMClose8DayAgo 
       , Close8DayAgo - Close9DayAgo AS DiffClose8DayAgoFROMClose9DayAgo 
       , Close1DayAgo - Close3DayAgo AS DiffClose1DayAgoFROMClose3DayAgo
       , Close1DayAgo - Close4DayAgo AS DiffClose1DayAgoFROMClose4DayAgo 
       , Close1DayAgo - Close5DayAgo AS DiffClose1DayAgoFROMClose5DayAgo 
       , Close1DayAgo - Close10DayAgo AS DiffClose1DayAgoFROMClose10DayAgo
       , Close9DayAgo - Close10DayAgo AS DiffClose9DayAgoFROMClose10DayAgo 
       , (High1DayAgo - Low1DayAgo) / (High2DayAgo - Low2DayAgo) AS Volatility1DayAgoOVERVolatility2DaysAgo 
       , (High2DayAgo - Low2DayAgo) / (High3DayAgo - Low3DayAgo) AS Volatility2DaysAgoOVERVolatility3DaysAgo 
       , (High3DayAgo - Low3DayAgo) / (High4DayAgo - Low4DayAgo) AS Volatility3DaysAgoOVERVolatility4DaysAgo 
       , (High4DayAgo - Low4DayAgo) / (High5DayAgo - Low5DayAgo) AS Volatility4DaysAgoOVERVolatility5DaysAgo 
       , (High5DayAgo - Low5DayAgo) / (High6DayAgo - Low6DayAgo) AS Volatility5DaysAgoOVERVolatility6DaysAgo
       , (High6DayAgo - Low6DayAgo) / (High7DayAgo - Low7DayAgo) AS Volatility6DaysAgoOVERVolatility7DaysAgo
       , (High7DayAgo - Low7DayAgo) / (High8DayAgo - Low8DayAgo) AS Volatility7DaysAgoOVERVolatility8DaysAgo 
       , (High8DayAgo - Low8DayAgo) / (High9DayAgo - Low9DayAgo) AS Volatility8DaysAgoOVERVolatility9DaysAgo 
       , (High9DayAgo - Low9DayAgo) / (High10DayAgo - Low10DayAgo) AS Volatility9DaysAgoOVERVolatility10DaysAgo
       , (High1DayAgo - Low1DayAgo) / (High3DayAgo - Low3DayAgo) AS Volatility1DayAgoOVERVolatility3DaysAgo
       , (High1DayAgo - Low1DayAgo) / (High4DayAgo - Low4DayAgo) AS Volatility1DayAgoOVERVolatility4DaysAgo 
       , (High1DayAgo - Low1DayAgo) / (High5DayAgo - Low5DayAgo) AS Volatility1DayAgoOVERVolatility5DaysAgo 
       , (High1DayAgo - Low1DayAgo) / (High10DayAgo - Low10DayAgo) AS Volatility1DayAgoOVERVolatility10DaysAgo 
       , (High10DayAgo - High7DayAgo) / (High3DayAgo - High1DayAgo) AS ThreeDayVolatilityBetweenHighTenAgoAndSevenAgoOVERTwoDayVolatilityBetweenHighThreeAgoAndOneAgo
       , (High9DayAgo - High6DayAgo) / (High3DayAgo - High1DayAgo) AS ThreeDayVolatilityBetweenHighNineAgoAndSixAgoOVERTwoDayVolatilityBetweenHighThreeAgoAndOneAgo 
       , (High8DayAgo - High5DayAgo) / (High3DayAgo - High1DayAgo) AS ThreeDayVolatilityBetweenHighEightAgoAndFiveAgoOVERTwoDayVolatilityBetweenHighThreeAgoAndOneAgo 
       , (High7DayAgo - High4DayAgo) / (High3DayAgo - High1DayAgo) AS ThreeDayVolatilityBetweenHighSevenAgoAndFourAgoOVERTwoDayVolatilityBetweenHighThreeAgoAndOneAgo 
 FROM stock_last_10_days""")


# COMMAND ----------

ignored_columns = ['id', 'label', 'date', 'open', 'high', 'low', 'volume', 'close', 'Adj Close', 'Date_Rank', 'DateRank1DayBehind', 'Date1DayBehind', \
         'DateRank2DayBehind', 'Date2DayBehind', 'Date3DayBehind', 'DateRank3DayBehind', 'DateRank4DayBehind', 'Date4DayBehind', 'Date5DayBehind', 'DateRank5DayBehind', \
         'DateRank6DayBehind', 'Date6DayBehind', 'Date7DayBehind', 'DateRank7DayBehind', 'DateRank8DayBehind', 'Date8DayBehind', 'Date9DayBehind', 'DateRank9DayBehind', \
         'DateRank10DayBehind','Date10DayBehind','HLAbsoluteVolatility', 'OCVolatility','Open1DayAgo', 'High1DayAgo', 'Low1DayAgo', 'Close1DayAgo', \
         'Open2DayAgo', 'High2DayAgo', 'Low2DayAgo', 'Close2DayAgo', 'Open3DayAgo', 'High3DayAgo', 'Low3DayAgo', 'Close3DayAgo', 'Open4DayAgo', 'High4DayAgo', \
         'Low4DayAgo', 'Close4DayAgo', 'Open5DayAgo', 'High5DayAgo', 'Low5DayAgo', 'Close5DayAgo', 'Open6DayAgo', 'High6DayAgo', 'Low6DayAgo', 'Close6DayAgo', \
         'Open7DayAgo', 'High7DayAgo', 'Low7DayAgo', 'Close7DayAgo', 'Open8DayAgo', 'High8DayAgo', 'Low8DayAgo', 'Close8DayAgo', 'Open9DayAgo', 'High9DayAgo', \
         'Low9DayAgo', 'Close9DayAgo', 'Open10DayAgo', 'High10DayAgo', 'Low10DayAgo', 'Close10DayAgo', 'BiRegrLabel']

#feature_cols_temp = training_df.columns
#feature_cols_temp = ['DiffHigh1DayAgoTOHigh2DayAgo', 'DiffHigh9DayAgoFROMHigh10DayAgo', 'DiffClose3DayAgoFROMClose4DayAgo', 'DiffClose5DayAgoFROMClose6DayAgo', 'DiffClose6DayAgoFROMClose7DayAgo',
#                     'DiffClose9DayAgoFROMClose10DayAgo']


#feature_selects = [col(c).cast("float") for c in feature_cols_temp if c not in ignored_columns]  
feature_cols = [c for c in features_df.columns if c not in ignored_columns] 

#dnf2 = training_df.select(*feature_selects, col("OCVolatility")).withColumnRenamed("OCVolatility", "label")
features_trimmed_df = features_df.select(*feature_cols).withColumnRenamed("BuyFlag", "label")
features_trimmed_df.createOrReplaceTempView("stock_features")

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /ml2/FirstAutoMLRun/inference

# COMMAND ----------

features_trimmed_df.write.mode("overwrite").parquet("/ml2/stock_features")

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

# COMMAND ----------

seed = 1800009193
(split_60_df, split_a_20_df, split_b_20_df) = features_trimmed_df.randomSplit([60.0, 20.0, 20.0], seed)
training_df = split_60_df

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol='features')

df2 = assembler.setHandleInvalid("skip").transform(features_trimmed_df)

mapping= {}
counter= 0 
for c in feature_cols:
  s = "0" + str(counter) 
  mapping.update({"features_" + s[-2:] : c})
  counter = counter + 1


df6 = df2.select(col("label").cast("double").alias("label"), col("features").alias("features"))

#display(df6)

#glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=0.3)
mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")

#model = glr.fit(df6)
model = mlr.fit(df6)

# print("Coefficients: " + str(model.coefficients))
# print("Intercept: " + str(model.intercept))
print("Coefficients: " + str(model.coefficientMatrix))
print("Intercept: " + str(model.interceptVector))

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

# Extract the summary from the returned LogisticRegressionModel instance trained
# in the earlier example
trainingSummary = model.summary

# Obtain the objective per iteration
objectiveHistory = trainingSummary.objectiveHistory
print("objectiveHistory:")
for objective in objectiveHistory:
    print(objective)

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
trainingSummary.roc.show()
print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

# Set the model threshold to maximize F-Measure
fMeasure = trainingSummary.fMeasureByThreshold
maxFMeasure = fMeasure.groupBy().max('F-Measure').select('max(F-Measure)').head()
bestThreshold = fMeasure.where(fMeasure['F-Measure'] == maxFMeasure['max(F-Measure)']) \
    .select('threshold').head()['threshold']
mlr.setThreshold(bestThreshold)

# COMMAND ----------

model.summary
# summary = model.summary
# print("Coefficient Standard Errors: " + str(summary.coefficientStandardErrors))
# print("T Values: " + str(summary.tValues))
# print("P Values: " + str(summary.pValues))
# print("Dispersion: " + str(summary.dispersion))
# print("Null Deviance: " + str(summary.nullDeviance))
# print("Residual Degree Of Freedom Null: " + str(summary.residualDegreeOfFreedomNull))
# print("Deviance: " + str(summary.deviance))
# print("Residual Degree Of Freedom: " + str(summary.residualDegreeOfFreedom))
# print("AIC: " + str(summary.aic))
# print("Deviance Residuals: ")
# summary.residuals().show()

# COMMAND ----------

import operator
sorted(mapping.items(), key=operator.itemgetter(0))

# COMMAND ----------

toy_df = spark.range(10).withColumn("predictor_one", col("id") % 2).withColumn("predictor_two", col("id") % 5).withColumn("predictor_three", (col("id") - .5) * .9)
assembler = VectorAssembler(
    inputCols=["predictor_two", "predictor_one"],
    outputCol='features')

assemble_df = assembler.setHandleInvalid("skip").transform(toy_df)
# display(assemble_df)

next_df = assemble_df.select(col("id").cast("double").alias("label"), col("features").alias("features"))

# display(next_df)

glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=0.3)
model = glr.fit(next_df)

model.summary

# COMMAND ----------


# display(dnf4)
#       , col("Open1DayAgo").cast("float")
#       , col("High1DayAgo").cast("float")
#       , col("Low1DayAgo").cast("float")
#       , col("Close1DayAgo").cast("float")
#       , col("Volume1DayAgo").cast("float")
#       , col("Open2DayAgo").cast("float")
#       , col("High2DayAgo").cast("float")
#       , col("Low2DayAgo").cast("float")
#       , col("Close2DayAgo").cast("float")
#       , col("Volume2DayAgo").cast("float")
#       , col("Open3DayAgo").cast("float")
#       , col("High3DayAgo").cast("float")
#       , col("Low3DayAgo").cast("float")
#       , col("Close3DayAgo").cast("float")
#       , col("Volume3DayAgo").cast("float")
#       , col("Open4DayAgo").cast("float")
#       , col("High4DayAgo").cast("float")
#       , col("Low4DayAgo").cast("float")
#       , col("Close4DayAgo").cast("float")
#       , col("Volume4DayAgo").cast("float")
#       , col("Open5DayAgo").cast("float")
#       , col("High5DayAgo").cast("float")
#       , col("Low5DayAgo").cast("float")
#       , col("Close5DayAgo").cast("float")
#       , col("Volume5DayAgo").cast("float")
#       , col("Open6DayAgo").cast("float")
#       , col("High6DayAgo").cast("float")
#       , col("Low6DayAgo").cast("float")
#       , col("Close6DayAgo").cast("float")
#       , col("Volume6DayAgo").cast("float")
#       , col("Open7DayAgo").cast("float")
#       , col("High7DayAgo").cast("float")
#       , col("Low7DayAgo").cast("float")
#       , col("Close7DayAgo").cast("float")
#       , col("Volume7DayAgo").cast("float")
#       , col("Open8DayAgo").cast("float")
#       , col("High8DayAgo").cast("float")
#       , col("Low8DayAgo").cast("float")
#       , col("Close8DayAgo").cast("float")
#       , col("Volume8DayAgo").cast("float")
#       , col("Open9DayAgo").cast("float")
#       , col("High9DayAgo").cast("float")
#       , col("Low9DayAgo").cast("float")
#       , col("Close9DayAgo").cast("float")
#       , col("Volume9DayAgo").cast("float")
#       , col("Open10DayAgo").cast("float")
#       , col("High10DayAgo").cast("float")
#       , col("Low10DayAgo").cast("float")
#       , col("Close10DayAgo").cast("float")
#       , col("Volume10DayAgo").cast("float")
#       , col("HLAbsoluteVolatility").cast("float")
#       , col("DiffHighTodayTOHigh2DayAgo").cast("float")
#       , col("DiffHigh2DayAgoFROMHigh3DayAgo").cast("float")
#       , col("DiffHigh3DayAgoFROMHigh4DayAgo").cast("float")
#       , col("DiffHigh4DayAgoFROMHigh5DayAgo").cast("float")
#       , col("DiffHigh5DayAgoFROMHigh6DayAgo").cast("float")
#       , col("DiffHigh6DayAgoFROMHigh7DayAgo").cast("float")
#       , col("DiffHigh7DayAgoFROMHigh8DayAgo").cast("float")
#       , col("DiffHigh8DayAgoFROMHigh9DayAgo").cast("float")
#       , col("DiffHigh9DayAgoFROMHigh10DayAgo").cast("float")
#       , col("DiffHighTodayFROMHigh3DayAgo").cast("float")
#       , col("DiffHighTodayFROMHigh4DayAgo").cast("float")
#       , col("DiffHighTodayFROMHigh5DayAgo").cast("float")
#       , col("DiffHighTodayFROMHigh6DayAgo").cast("float")
#       , col("DiffCloseTodayFROMClose2DayAgo").cast("float")
#       , col("DiffClose2DayAgoFROMClose3DayAgo").cast("float")
#       , col("DiffClose3DayAgoFROMClose4DayAgo").cast("float")
#       , col("DiffClose4DayAgoFROMClose5DayAgo").cast("float")
#       , col("DiffClose5DayAgoFROMClose6DayAgo").cast("float")
#       , col("DiffClose6DayAgoFROMClose7DayAgo").cast("float")
#       , col("DiffClose7DayAgoFROMClose8DayAgo").cast("float")
#       , col("DiffClose8DayAgoFROMClose9DayAgo").cast("float")
#       , col("DiffClose9DayAgoFROMClose10DayAgo").cast("float")
#       , col("DiffCloseTodayFROMClose3DayAgo").cast("float")
#       , col("DiffCloseTodayFROMClose4DayAgo").cast("float")
#       , col("DiffCloseTodayFROMClose5DayAgo").cast("float")
#       , col("DiffCloseTodayFROMClose6DayAgo").cast("float")
#       , col("VolatilityTodayOVERVolatility2DaysAgo").cast("float")
#       , col("Volatility2DaysAgoOVERVolatility3DaysAgo").cast("float")
#       , col("Volatility3DaysAgoOVERVolatility4DaysAgo").cast("float")
#       , col("Volatility4DaysAgoOVERVolatility5DaysAgo").cast("float")
#       , col("Volatility5DaysAgoOVERVolatility6DaysAgo").cast("float")
#       , col("Volatility6DaysAgoOVERVolatility7DaysAgo").cast("float")
#       , col("Volatility7DaysAgoOVERVolatility8DaysAgo").cast("float")
#       , col("Volatility8DaysAgoOVERVolatility9DaysAgo").cast("float")
#       , col("Volatility9DaysAgoOVERVolatility10DaysAgo").cast("float")
#       , col("VolatilityTodayOVERVolatility3DaysAgo").cast("float")
#       , col("VolatilityTodayOVERVolatility4DaysAgo").cast("float")
#       , col("VolatilityTodayOVERVolatility5DaysAgo").cast("float")
#       , col("VolatilityTodayOVERVolatility10DaysAgo").cast("float"))

# display(dnf4)

# ignore = ['id', 'label', 'Date', 'Open', 'High', 'Low', 'Volume', 'Adj Close', 'Date_Rank', 'DateRank1DayBehind', 'Date1DayBehind' \
#          'DateRank2DayBehind', 'Date2DayBehind', 'Date3DayBehind', 'DateRank3DayBehind', 'DateRank4DayBehind', 'Date4DayBehind', 'Date5DayBehind', 'DateRank5DayBehind' \
#          'DateRank6DayBehind', 'Date6DayBehind', 'Date7DayBehind', 'DateRank7DayBehind', 'DateRank8DayBehind', 'Date8DayBehind', 'Date9DayBehind', 'DateRank9DayBehind' \
#          'HLAbsoluteVolatility', 'OCVolatility']
