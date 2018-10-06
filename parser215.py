# 載入函式庫
from pyspark.sql import *
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

# 讀入資料
dfSql = sqlContext.read.csv("Ubus215.csv",  header=True)

# 變更資料型態
dfSql = dfSql.withColumn("Qty", dfSql["Qty"].cast(IntegerType()))
dfSql = dfSql.withColumn("TDate", to_date(dfSql["TDate"], "yyyyMMdd").cast("date"))


# 取出客戶代號
listCustomer = dfSql.select("CusAUnt").distinct().collect()
# 取出車輛牌照號碼
listCar = dfSql.select("CarNo").distinct().collect()

# 取出加油站代號
listStd = dfSql.select("StdNo").distinct().collect()

# 取出加油日期
listTdate = dfSql.select("TDate").distinct().sort(dfSql["TDate"])

yearRange = []
Range = []

for idxItem in listTdate.collect():
  idxItem[0].year
  idxItem[0].month
  idxItem[0].day

# 取出車輛的加油地點
for idxCar in range(len(listCar)):
  print(idxCar, listCar[idxCar][0])
  dfSql.where(dfSql["CarNo"] == listCar[idxCar][0]).select('StdNo').distinct().collect()
  dfSql.where(dfSql["CarNo"] == listCar[idxCar][0]).select("Qty").toPandas().sum()

dateFrom = "2016-03-01"
dateTo = "2016-03-10"

# 取出車輛的加油地點，有打錨點
for idxCar in range(len(listCar)):
  # print(idxCar, listCar[idxCar][0])
  tmpDfSql = dfSql.where(dfSql["CarNo"] == listCar[idxCar][0]).persist()
  # tmpDfSql.select('StdNo').distinct().collect()
  # tmpDfSql.select("Qty").toPandas().sum()
  # tmpDfSql.where((tmpDfSql["TDate"] >= dateFrom) & (tmpDfSql["TDate"] <= dateTo)).collect()
  # tmpDfSql.select(tmpDfSql["TDate"]).distinct().show()
  # tmpDfSql.select(tmpDfSql["TDate"]).distinct().orderBy(tmpDfSql["TDate"]).show()


