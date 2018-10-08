#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

# 載入函式庫
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sc = SparkContext()
sqlContext = SQLContext(sc)

# 定義資料結構
schema = StructType([
  StructField("Deptno", StringType(), True),
  StructField("Island", StringType(), True),
  StructField("Gun_No", StringType(), True),
  StructField("Tran_Time", TimestampType(), True),
  StructField("Seq", StringType(), True),
  StructField("Tax_Type", StringType(), True),
  StructField("Product_ID", StringType(), True),
  StructField("Class", StringType(), True),
  StructField("Price", FloatType(), True),
  StructField("Amt", FloatType(), True),
  StructField("Qty", FloatType(), True),
  StructField("Unit", StringType(), True),
  StructField("Ref_No", StringType(), True)
])

# 讀入原始資料，使用預先設定結構
inputPath = "/home/mywh/data"
inputFileName = "tran_detl.csv"
dfTranDetl = sqlContext.read.csv("/home/mywh/data/tran_detl.csv", header=True, schema=schema)

# 設定資料擷取之日期區間
datetimeRange = str(sys.argv[1])
datetimeFrom = datetimeRange + " " + "00:00:00"
datetimeTo = datetimeRange + " " + "23:59:59"
tmpDfTranDelt = dfTranDetl.where((dfTranDetl["Tran_Time"] >= datetimeFrom) & (dfTranDetl["Tran_Time"] <= datetimeTo))

# 寫入檔案、多檔模式、CSV 格式
outputPath = "/home/mywh/data"
outputFileName = datetimeRange + "_TranDelt.csv"
tmpDfTranDelt.write.csv(outputPath + "/" + outputFileName)
