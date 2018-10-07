# 載入函式庫
from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(sc)

#
listYear = [2018]
listMonth = range(1, 13)
listDay = range(1, 32)

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
dfTranDetl = sqlContext.read.csv("tran_detl.csv", header=True, schema=schema)

for idxY in listYear:
  tmpDatetimeYear = str(idxY) + "-"
  #
  for idxM in listMonth:
    if (idxM >= 10):
      tmpDatetimeMonth = str(idxM) + "-"
    else:
      tmpDatetimeMonth = "0" + str(idxM) + "-"        
    #
    for idxD in listDay:
      if (idxD >= 10):
        tmpDatetimeDay = str(idxD)
      else:
        tmpDatetimeDay = "0" + str(idxD)
      #
      datetimeFrom = tmpDatetimeYear + tmpDatetimeMonth + tmpDatetimeDay + " 00:00:00"
      datetimeTo = tmpDatetimeYear + tmpDatetimeMonth + tmpDatetimeDay + " 23:59:59"
      print (datetimeFrom, datetimeTo)
      
      # 取出特定日期
      # datetimeFrom = "2018-02-01 00:00:00"
      # datetimeTo = "2018-02-01 23:59:59"
      tmpDfTranDelt = dfTranDetl.where((dfTranDetl["Tran_Time"] >= datetimeFrom) & (dfTranDetl["Tran_Time"] <= datetimeTo)).persist()
      
      # 寫入檔案、多檔模式、CSV 格式
      fileName = tmpDatetimeYear + tmpDatetimeMonth + tmpDatetimeDay + "_TranDelt.csv"
      tmpDfTranDelt.write.csv(fileName)

datetimeFrom = "2018-01-03 00:00:00"
datetimeTo = "2018-01-03 23:59:59"
tmpDfTranDelt = dfTranDetl.where((dfTranDetl["Tran_Time"] >= datetimeFrom) & (dfTranDetl["Tran_Time"] <= datetimeTo)).persist()
# 寫入檔案、多檔模式、CSV 格式
tmpDfTranDelt.write.csv('20180103_TranDelt.csv')
      
# 寫入檔案、單一檔案（註：可能導致存檔失效；無法充分使用分配的計算資源）、CSV 格式
tmpDfTranDelt.coalesce(1).write.csv('20180201_TranDelt.csv')

# 自 Tran_Time 取出日期
splitCol = pyspark.sql.functions.split(tmpDfTranDelt["Tran_Time"], '-')
tmpDfTranDelt = tmpDfTranDelt.withColumn('Date', splitCol.getItem(0))
tmpDfTranDelt = tmpDfTranDelt.withColumn('Time', splitCol.getItem(1))
tmpDfTranDelt = tmpDfTranDelt.withColumn("Date", to_date(tmpDfTranDelt["Date"], "yyyy-MM-dd").cast("date"))

