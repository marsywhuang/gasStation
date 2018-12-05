# 載入環境
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession

# 載入函式庫
from pyspark.sql.functions import sum
from pyspark.sql.functions import count
from pyspark.sql.functions import col
from pyspark.sql.functions import when

#
# 匯入自營站油銷明細資料
#

# 來源路徑
inputPathL1 = "/home/cpc/data/rawData"
inputPathL2 = "tranDelt/tranDelt_*/tranDelt_*/tranDelt_*"
inputPath = inputPathL1 + "/" + inputPathL2

# 來源資料
inputFile = "p*"

# 完整路徑和資料
inputFull = inputPath + "/" + inputFile

# 讀入來源資料
df = sqlContext.read.csv(inputFull, encoding = "utf-8", header = "true")

#
# 坤神要的
#

#
# 3.1 各油品銷售次數：加油站-年-月-日-油品-筆數
#

# 產品
productidColumn = [
  "113F 1209800", "113F 1209500", "113F 1209200", "113F 1229500",
  "113F 5100100",
  "113F 5100700", "113F 5100800"]

# 表列要統計的欄位名稱
statColumn = ["Deptno", "Tran_Time", "Product_ID"]

# 日期欄位
dtColumn = ["dateYear", "dateMonth", "dateDay"]

# 取出特定欄位
pDf = df.select(statColumn)

# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn('dateYear', pDf['Tran_Time'].substr(1, 4))
       .withColumn('dateMonth', pDf['Tran_Time'].substr(6, 2))
       .withColumn('dateDay', pDf['Tran_Time'].substr(9, 2)))

# 刪除不必要欄位
tDf = tDf.drop(tDf.Tran_Time)

# 群組欄位
groupColumn = ["Deptno", "dateYear", "dateMonth", "dateDay"]

# 加油站－年－月－日》計算汽油及柴油產品筆數
deptnoYMDcProductid = (
       tDf
       .groupBy(groupColumn)
       .agg(count(when((col("Product_ID").contains(productidColumn[0])), True)).alias("cnt" + productidColumn[0]),
            count(when((col("Product_ID").contains(productidColumn[1])), True)).alias("cnt" + productidColumn[1]),
            count(when((col("Product_ID").contains(productidColumn[2])), True)).alias("cnt" + productidColumn[2]),
            count(when((col("Product_ID").contains(productidColumn[3])), True)).alias("cnt" + productidColumn[3]),
            count(when((col("Product_ID").contains(productidColumn[4])), True)).alias("cnt" + productidColumn[4]),
            count(when((col("Product_ID").contains(productidColumn[5])), True)).alias("cnt" + productidColumn[5]),
            count(when((col("Product_ID").contains(productidColumn[6])), True)).alias("cnt" + productidColumn[6]))
       .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoItemGasDieselYMDcProductid"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDcProductid.write.format("json").save(outputFull)

#
# 3.2 汽、機車加油筆數：加油站-年-月-日-車種（Amt > 249 | Amt < 250）-筆數
#

# 產品
productidColumn = [
  "113F 1209800", "113F 1209500", "113F 1209200", "113F 1229500",
  "113F 5100100",
  "113F 5100700", "113F 5100800"]

# 取出符合產品項目的資料記錄
fDf = df.where(df.Product_ID.contains(productidColumn[0]) |
               df.Product_ID.contains(productidColumn[1]) |
               df.Product_ID.contains(productidColumn[2]) |
               df.Product_ID.contains(productidColumn[3]) |
               df.Product_ID.contains(productidColumn[4]) |
               df.Product_ID.contains(productidColumn[5]) |
               df.Product_ID.contains(productidColumn[6]))

# 表列要統計的欄位名稱
statColumn = ["Deptno", "Tran_Time", "Amt"]

# 取出特定欄位
pDf = fDf.select(statColumn)

# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn("dateYear", pDf["Tran_Time"].substr(1, 4))
       .withColumn("dateMonth", pDf["Tran_Time"].substr(6, 2))
       .withColumn("dateDay", pDf["Tran_Time"].substr(9, 2)))

# 刪除不必要欄位
tDf = tDf.drop(tDf.Tran_Time)

# 群組欄位
groupColumn = ["Deptno", "dateYear", "dateMonth", "dateDay"]

# 加油站－年－月－日》計算交易金額在（1）小於等於249及（2）大於等於250的筆數
deptnoYMDcAmt = (tDf
                 .groupBy(groupColumn)
                 .agg(count(when((col("Amt").cast("float") < 250), True)).alias("cntBike"),
                      count(when((col("Amt").cast("float") >= 250), True)).alias("cntCar"))
                 .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoItemGasDieselYMDcAmt"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDcAmt.write.format('json').save(outputFull)

#
# 3.3 加油站的發油量：加油站-年-月-日-發油量
#

# 產品
productidColumn = [
  "113F 1209800", "113F 1209500", "113F 1209200", "113F 1229500",
  "113F 5100100",
  "113F 5100700", "113F 5100800"]

# 取出符合產品項目的資料記錄
fDf = df.where(df.Product_ID.contains(productidColumn[0]) |
               df.Product_ID.contains(productidColumn[1]) |
               df.Product_ID.contains(productidColumn[2]) |
               df.Product_ID.contains(productidColumn[3]) |
               df.Product_ID.contains(productidColumn[4]) |
               df.Product_ID.contains(productidColumn[5]) |
               df.Product_ID.contains(productidColumn[6]))

# 表列要統計的欄位名稱
statColumn = ["Deptno", "Tran_Time", "Qty"]

# 取出特定欄位
pDf = fDf.select(statColumn)

# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn("dateYear", pDf["Tran_Time"].substr(1, 4))
       .withColumn("dateMonth", pDf["Tran_Time"].substr(6, 2))
       .withColumn("dateDay", pDf["Tran_Time"].substr(9, 2)))

# 刪除不必要欄位
tDf = tDf.drop(tDf.Tran_Time)

# 群組欄位
groupColumn = ["Deptno", "dateYear", "dateMonth", "dateDay"]

#
deptnoYMDsQty = (tDf
                 .groupBy(groupColumn)
                 .agg(sum(tDf.Qty.cast("float")).alias("sumQty"))
                 .orderBy(groupColumn))


# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoTotalGasDieselYMDsQty"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDsQty.write.format('json').save(outputFull)

#
# 3.4 加油站的發油量：加油站－年-月-日-各項油品發油量總數
#

# 產品
productidColumn = [
  "113F 1209800", "113F 1209500", "113F 1209200", "113F 1229500",
  "113F 5100100",
  "113F 5100700", "113F 5100800"]

# 取出符合產品項目的資料記錄
fDf = df.where(df.Product_ID.contains(productidColumn[0]) |
               df.Product_ID.contains(productidColumn[1]) |
               df.Product_ID.contains(productidColumn[2]) |
               df.Product_ID.contains(productidColumn[3]) |
               df.Product_ID.contains(productidColumn[4]) |
               df.Product_ID.contains(productidColumn[5]) |
               df.Product_ID.contains(productidColumn[6]))

# 表列要統計的欄位名稱
statColumn = ["Deptno", "Tran_Time", "Qty", "Product_ID"]

# 取出特定欄位
pDf = fDf.select(statColumn)

# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn("dateYear", pDf["Tran_Time"].substr(1, 4))
       .withColumn("dateMonth", pDf["Tran_Time"].substr(6, 2))
       .withColumn("dateDay", pDf["Tran_Time"].substr(9, 2)))

# 刪除不必要欄位
tDf = tDf.drop(tDf.Tran_Time)

# 群組欄位
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay']

# 加油站－年－月－日》各項油品發油量總數
deptnoYMDsQty = (tDf
                 .groupBy(groupColumn)
                 .agg(sum(when((col("Product_ID").contains(productidColumn[0])),
                               tDf.Qty.cast("float"))).alias("sum" + productidColumn[0]),
                      sum(when((col("Product_ID").contains(productidColumn[1])),
                               tDf.Qty.cast("float"))).alias("sum" + productidColumn[1]),
                      sum(when((col("Product_ID").contains(productidColumn[2])),
                               tDf.Qty.cast("float"))).alias("sum" + productidColumn[2]),
                      sum(when((col("Product_ID").contains(productidColumn[3])),
                               tDf.Qty.cast("float"))).alias("sum" + productidColumn[3]),
                      sum(when((col("Product_ID").contains(productidColumn[4])),
                               tDf.Qty.cast("float"))).alias("sum" + productidColumn[4]),
                      sum(when((col("Product_ID").contains(productidColumn[5])),
                               tDf.Qty.cast("float"))).alias("sum" + productidColumn[5]),
                      sum(when((col("Product_ID").contains(productidColumn[6])),
                               tDf.Qty.cast("float"))).alias("sum" + productidColumn[6]))
                 .orderBy(groupColumn))                 

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoEachGasDieselYMDsQty"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDsQty.write.format('json').save(outputFull)
