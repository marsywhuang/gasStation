# 載入函式庫
from pyspark.sql import *
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

# 讀入資料
dfSql = sqlContext.read.csv("tranMaster_201801.csv",  header=False)

# 變更資料型態
dfSql = dfSql.withColumn("_c3", col("_c3").cast('timestamp'))

dateFrom = "2018-01-01"
timeFrom = "00:00:00"
timeTo = "23:59:59"
dfSql.where((dfSql["_c3"] >= (dateFrom + " " + timeFrom)) & (dfSql["_c3"] <= (dateFrom + " " + timeTo))).count()
