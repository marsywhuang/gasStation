# 載入函式庫
from pyspark.sql import *
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

# 讀入資料
dfSql = sqlContext.read.csv("tranMaster_201801.csv",  header=False)

# 變更資料型態
dfSql = dfSql.withColumn("_c3", col("_c3").cast('timestamp'))


# 設定起始時間
timeFrom = "00:00:00"
# 設定終止時間
timeTo = "23:59:59"
# 設定儲存目錄路徑
outputPath = "/home/mywh/data"
#
for idxDay in range(1, 32):
  #
  dateFrom = "2018-01"
  #
  if (idxDay < 10):
    dateFrom = dateFrom + "-0" + str(idxDay)
  else:
    dateFrom = dateFrom + "-" + str(idxDay)
  #
  tmpDateTimeFrom = dateFrom + " " + timeFrom
  tmpDateTimeTo = dateFrom + " " + timeTo
  #
  tmpDfSqlTranMaster = dfSql.where((dfSql["_c3"] >= tmpDateTimeFrom) & (dfSql["_c3"] <= tmpDateTimeTo))
  
  # 寫入檔案、多檔模式、CSV 格式
  outputFileName = "tranMaster-" + dateFrom + ".csv"
  tmpDfSqlTranMaster.write.csv(outputPath + "/" + outputFileName)

# tmpDfSqlTranMaster.coalesce(1).write.csv(outputPath + "/" + outputFileName)
