# 載入函式庫
from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(sc)


# 定義資料結構
schema = StructType([
  StructField("RID", StringType(), True),
  StructField("CType", StringType(), True),
  StructField("StdNo", StringType(), True),
  StructField("PNo", StringType(), True),
  StructField("Unt", StringType(), True),
  StructField("LDate", StringType(), True),
  StructField("TDate", TimestampType(), True),
  StructField("Qty", FloatType(), True),
  StructField("StrNo", StringType(), True),
  StructField("TNo", StringType(), True),
  StructField("Mrk3", StringType(), True),
  StructField("Mrk4", StringType(), True),
  StructField("Mrk5", StringType(), True),
  StructField("TicketNo", StringType(), True),
  StructField("Ship", StringType(), True),
  StructField("CarNo", StringType(), True),
  StructField("PName", StringType(), True),
  StructField("TicketType", StringType(), True),
  StructField("CusAUnt", StringType(), True),
  StructField("CusMUnt", StringType(), True),
  StructField("TTime", StringType(), True),
  StructField("CardMNo", StringType(), True),
  StructField("CTypeMk", StringType(), True),
  StructField("Mile", StringType(), True),
  StructField("Price", StringType(), True),
  StructField("CNo", StringType(), True),
  StructField("BillNo", StringType(), True),
  StructField("AType", StringType(), True),
  StructField("ADate", StringType(), True),
  StructField("rDate", StringType(), True),
  StructField("MDate", StringType(), True),
  StructField("SID", StringType(), True),
  StructField("YYMM", StringType(), True),
  StructField("Bprice", StringType(), True),
  StructField("Sprice", StringType(), True),
  StructField("Mk1", StringType(), True),
  StructField("Mk2", StringType(), True),
  StructField("Mk3", StringType(), True),
  StructField("S3_SEQNO", StringType(), True),
  StructField("ISLAND_SEQNO", StringType(), True),
  StructField("EDC_VERSION", StringType(), True),
  StructField("FLT_BATCH_NO", StringType(), True),
  StructField("cust_type", StringType(), True),
  StructField("stn_type", StringType(), True),
  StructField("ksmprice", StringType(), True),
  StructField("rksmprice", StringType(), True),
  StructField("rstnprice", StringType(), True)
])

# 讀入原始資料，使用預先設定結構
dfUbus = sqlContext.read.csv("Ubus215.csv", header=True, schema=schema)

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
