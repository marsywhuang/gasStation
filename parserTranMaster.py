# 載入函式庫
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


# 定義資料結構
schema = StructType([
  StructField("Deptno", StringType(), True),
  StructField("Island", StringType(), True),
  StructField("Gun_No", StringType(), True),
  StructField("Tran_time", TimestampType(), True),
  StructField("Tax_Type", StringType(), True),
  StructField("Tran_Amt", StringType(), True),
  StructField("Real_Amt", StringType(), True),
  StructField("Inv_Amt", StringType(), True),
  StructField("Tax", StringType(), True),
  StructField("Discount", StringType(), True),
  StructField("Person_Id", StringType(), True),
  StructField("Uid", StringType(), True),
  StructField("Invoice_Tp", StringType(), True),
  StructField("Invoice", StringType(), True),
  StructField("Invoice_Cnt", StringType(), True),
  StructField("Ticket_Amt", StringType(), True),
  StructField("Payment", StringType(), True),
  StructField("Tran_Tp", StringType(), True),
  StructField("Car_No", StringType(), True),
  StructField("VIP_No", StringType(), True),
  StructField("Shift", StringType(), True),
  StructField("Ref_No1", StringType(), True),
  StructField("Ref_No2", StringType(), True),
  StructField("Ref_No3", StringType(), True),
  StructField("Ref_No4", StringType(), True),
  StructField("Ser_No", StringType(), True),
  StructField("VOID", StringType(), True),
  StructField("Prt_Invoice", StringType(), True),
  StructField("An_Mark", StringType(), True),
  StructField("An_Ref1", StringType(), True),
  StructField("An_Ref2", StringType(), True),
  StructField("Tran_Mode", StringType(), True),
  StructField("Ref_No5", StringType(), True),
  StructField("Vip_Apoint", StringType(), True),
  StructField("Vip_Cpoint", StringType(), True),
  StructField("Ref_No6", StringType(), True),
  StructField("Ref_No7", StringType(), True),
  StructField("Ref_No8", StringType(), True),
  StructField("Vol_Num", FloatType(), True),
  StructField("Amt_Num", StringType(), True),
  StructField("Print_Mark", StringType(), True),
  StructField("Carrier_Id1", StringType(), True),
  StructField("Carrier_Id2", StringType(), True),
  StructField("Random_Num", StringType(), True),
  StructField("Cancel_Reason", StringType(), True),
  StructField("Ref_No9", StringType(), True),
  StructField("Ref_No10", StringType(), True),
  StructField("Ecr_No", StringType(), True),
  StructField("Sha_Card_No", StringType(), True),
  StructField("Reverse_fg", StringType(), True),
  StructField("Ref_No11", StringType(), True),
  StructField("Ref_No12", StringType(), True),
  StructField("Ref_No13", StringType(), True),
  StructField("Ref_No14", StringType(), True),
  StructField("Ref_No15", StringType(), True),
  StructField("Ref_No16", StringType(), True),
  StructField("Ref_No17", StringType(), True)
])

# 讀入資料
inputPath = "/home/mywh/data"
inputFileName = "tranMaster_201801.csv"
dfSql = sqlContext.read.csv(inputPath + "/" + inputFileName,  header=False, schema=schema)

# 取出加油站，去除重覆項
listDeptNo = tmpDfSqlTranMaster.select(tmpDfSqlTranMaster["Deptno"]).distinct().collect()

# 取出車號，去除重覆項
listCarNo = tmpDfSqlTranMaster.select(tmpDfSqlTranMaster["Car_No"]).distinct().collect()

#
dfSql.where(dfSql["Car_No"] == idxItem[0]).groupby(dfSql["Car_No"]).agg(collect_list(struct("Deptno", "Car_No"))).show()

# 根據車號進行分群（註：以單檔大小接近 10 GB，會出現錯誤
tmpDfSql = dfSql.groupby(dfSql["Car_No"]).agg(collect_list(struct(dfSql.columns)).alias("message"))


for idxItem in listCarNo:
  dfSql.where(dfSql["Car_No"] == idxItem[0]).count()





# 變更資料型態
dfSql = dfSql.withColumn("_c3", col("_c3").cast('timestamp'))


# 設定起始時間
timeFrom = "00:00:00"
# 設定終止時間
timeTo = "23:59:59"
# 設定儲存目錄路徑
outputPath = "/home/mywh/data"
# 設定日期區間－日
for idxDay in range(1, 32):
  # 設定日期區間－年月
  dateFrom = "2018-01"
  # 補零
  if (idxDay < 10):
    dateFrom = dateFrom + "-0" + str(idxDay)
  else:
    dateFrom = dateFrom + "-" + str(idxDay)
  # 起迄日期區間
  tmpDateTimeFrom = dateFrom + " " + timeFrom
  tmpDateTimeTo = dateFrom + " " + timeTo
  # 
  tmpDfSql = dfSql.where((dfSql["Tran_time"] >= tmpDateTimeFrom) & (dfSql["Tran_time"] <= tmpDateTimeTo))
  
  # 寫入檔案、多檔模式、CSV 格式
  outputFileName = "tranMaster-" + dateFrom + ".csv"
  tmpDfSql.write.csv(outputPath + "/" + outputFileName)

# tmpDfSql.coalesce(1).write.csv(outputPath + "/" + outputFileName)
