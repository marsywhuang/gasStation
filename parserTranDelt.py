# 載入函式庫
from pyspark.sql import SQLContext
from pyspark.sql.types import *
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
  StructField("Price", StringType(), True),
  StructField("Amt", StringType(), True),
  StructField("Qty", FloatType(), True),
  StructField("Unit", StringType(), True),
  StructField("Ref_No", StringType(), True)
])

# 讀入原始資料
dfTranDetl = sqlContext.read.csv("tran_detl.csv", header=True, schema=schema)
