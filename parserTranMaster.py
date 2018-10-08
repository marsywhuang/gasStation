# 載入函式庫
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

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

# 載入數據
dfTranMaster = sqlContext.read.csv("/home/mywh/data/tranMaster_201801.csv", header=False)


for idxItem in dfTranMaster.columns:
  print ("Name", idxItem)
  # 下錨點
  tmpDfTranMaster = dfTranMaster.select(idxItem).distinct().persist()
  # 計算不重覆個數
  tmpDfTranMaster.count()
  # 取出前三項
  tmpDfTranMaster.take(3)

