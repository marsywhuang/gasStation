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
# 匯入來源資料
#

# 來源路徑
inputPath = "/home/cpc/data/tranDelt/tranDelt_*/tranDelt_*/tranDelt_*"
# 來源資料
inputFile = "p*"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile
# 讀入來源資料
df = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "true")

# 來源路徑
inputPath = "/home/cpc/data/rawData"
# 來源資料
inputFile = "gasInfo.csv"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile
# 讀入來源資料
dfGasInfo = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "true")

#
# 加油站－年－月－日》計算油銷總量
#

# 表列要統計的欄位名稱
statColumn = ['Deptno', 'Tran_Time', 'Qty']

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
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay']
# 加油站－年－月－日》計算油銷總量
deptnoYMDaQty = (tDf
                 .groupBy(groupColumn)
                 .agg(sum(tDf.Qty.cast('float')).alias('aQty'))
                 .orderBy(groupColumn))


# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoYMDaQty.json"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
stdnoPaymentYearDf.write.format('json').save(outputFull)

#
# 加油站－年－月－日－產品》計數（筆數）
#

# 表列要統計的欄位名稱
statColumn = ['Deptno', 'Tran_Time', 'Product_ID']

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
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay', 'ProductId']
# 加油站－產品－年－月－日》計數（筆數）
deptnoYMDaProductid = (tDf
                       .groupBy(groupColumn)
                       .agg(count(tDf.ProductId).alias('cProductId'))
                       .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoYMDaProductid.json"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoProductidYearMonthDayDf.write.format('json').save(outputFull)

#
# 加油站－金額（小於等於249）－年－月－日》計數（筆數）
# 加油站－金額（大於250）－年－月－日》計數（筆數）
#

# 表列要統計的欄位名稱
statColumn = ['Deptno', 'Tran_Time', 'Amt']
# 取出特定欄位
pDf = df.select(statColumn)
# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn('dateYear', pDf['Tran_Time'].substr(1, 4))
       .withColumn('dateMonth', pDf['Tran_Time'].substr(6, 2))
       .withColumn('dateDay', pDf['Tran_Time'].substr(9, 2)))
# 刪除不必要欄位
tDf = tDf.drop(tDf.Tran_Time)
#
# 群組欄位
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay']
# 加油站－年－月－日》計算交易金額在（1）小於等於249及（2）大於等於250的筆數
deptnoYMDaAmt = (tDf
                 .groupBy(groupColumn)
                 .agg(count(when((col("Amt").cast('float') < 250), True)).alias('aBike'),
                      count(when((col("Amt").cast('float') >= 250), True)).alias('aCar'))
                 .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoYMDaAmt.json"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDaAmt.write.format('json').save(outputFull)

#
# 加油站－年－月》計算各類產品筆數
#

# 產品
productidColumn = ['113F 1209800', '113F 1209500', '113F 1209200', '113F 1229500',
                   '113F 5100100',
                   '113F 5100700', '113F 5100800']
# 表列要統計的欄位名稱
statColumn = ['Deptno', 'Tran_Time', 'Product_ID']
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
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay']
# 加油站－年－月》計算各類產品筆數
deptnoYMDaProductid = (tDf
                       .groupBy(groupColumn)
                       .agg(count(when((col("Product_ID").contains(productidColumn[0])), True)).alias('a'+productidColumn[0]),
                            count(when((col("Product_ID").contains(productidColumn[1])), True)).alias('a'+productidColumn[1]),
                            count(when((col("Product_ID").contains(productidColumn[2])), True)).alias('a'+productidColumn[2]),
                            count(when((col("Product_ID").contains(productidColumn[3])), True)).alias('a'+productidColumn[3]),
                            count(when((col("Product_ID").contains(productidColumn[4])), True)).alias('a'+productidColumn[4]),
                            count(when((col("Product_ID").contains(productidColumn[5])), True)).alias('a'+productidColumn[5]),
                            count(when((col("Product_ID").contains(productidColumn[6])), True)).alias('a'+productidColumn[6]))
                       .orderBy(groupColumn))
# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoYMDaProductid.json"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDaProductid.write.format('json').save(outputFull)

#
# 加油站－年－月》計算各類類別筆數
#

# 類別
classColumn = ['100', '101', '102', '103', '104', '105', '106', '107',
               '121', '122',
               '131', '132', '133']

# 表列要統計的欄位名稱
statColumn = ['Deptno', 'Tran_Time', 'Class']

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
groupColumn = ['Deptno', 'dateYear', 'dateMonth']
# 加油站－年－月》計算各類類別筆數
deptnoYMDaClass = (tDf
                   .groupBy(groupColumn)
                   .agg(count(when((col("Class") == classColumn[0]), True)).alias('a'+classColumn[0]),
                        count(when((col("Class") == classColumn[1]), True)).alias('a'+classColumn[1]),
                        count(when((col("Class") == classColumn[2]), True)).alias('a'+classColumn[2]),
                        count(when((col("Class") == classColumn[3]), True)).alias('a'+classColumn[3]),
                        count(when((col("Class") == classColumn[4]), True)).alias('a'+classColumn[4]),
                        count(when((col("Class") == classColumn[5]), True)).alias('a'+classColumn[5]),
                        count(when((col("Class") == classColumn[6]), True)).alias('a'+classColumn[6]),
                        count(when((col("Class") == classColumn[7]), True)).alias('a'+classColumn[7]),
                        count(when((col("Class") == classColumn[8]), True)).alias('a'+classColumn[8]),
                        count(when((col("Class") == classColumn[9]), True)).alias('a'+classColumn[9]),
                        count(when((col("Class") == classColumn[10]), True)).alias('a'+classColumn[10]),
                        count(when((col("Class") == classColumn[11]), True)).alias('a'+classColumn[11]),
                        count(when((col("Class") == classColumn[12]), True)).alias('a'+classColumn[12]))
                   .orderBy(groupColumn))

#
# 坤神要的
#

# 3.1 各油品銷售次數：加油站-年-月-日-油品-筆數
# 產品
productidColumn = ['113F 1209800', '113F 1209500', '113F 1209200', '113F 1229500',
                   '113F 5100100',
                   '113F 5100700', '113F 5100800']
# 表列要統計的欄位名稱
statColumn = ['Deptno', 'Tran_Time', 'Product_ID']
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
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay']
# 加油站－年－月－日》計算汽油及柴油產品筆數
deptnoYMDcProductid = (tDf
                       .groupBy(groupColumn)
                       .agg(count(when((col("Product_ID").contains(productidColumn[0])), True)).alias('c'+productidColumn[0]),
                            count(when((col("Product_ID").contains(productidColumn[1])), True)).alias('c'+productidColumn[1]),
                            count(when((col("Product_ID").contains(productidColumn[2])), True)).alias('c'+productidColumn[2]),
                            count(when((col("Product_ID").contains(productidColumn[3])), True)).alias('c'+productidColumn[3]),
                            count(when((col("Product_ID").contains(productidColumn[4])), True)).alias('c'+productidColumn[4]),
                            count(when((col("Product_ID").contains(productidColumn[5])), True)).alias('c'+productidColumn[5]),
                            count(when((col("Product_ID").contains(productidColumn[6])), True)).alias('c'+productidColumn[6]))
                       .orderBy(groupColumn))
# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoItemGasDieselYMDcProductid"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDcProductid.write.format('json').save(outputFull)

######

# 群組欄位
groupColumn = ['Deptno', 'dateYear', 'dateMonth']
# 加油站－年－月》計算汽油及柴油產品筆數
deptnoYMcProductid = (tDf
                       .groupBy(groupColumn)
                       .agg(count(when((col("Product_ID").contains(productidColumn[0])), True)).alias('c'+productidColumn[0]),
                            count(when((col("Product_ID").contains(productidColumn[1])), True)).alias('c'+productidColumn[1]),
                            count(when((col("Product_ID").contains(productidColumn[2])), True)).alias('c'+productidColumn[2]),
                            count(when((col("Product_ID").contains(productidColumn[3])), True)).alias('c'+productidColumn[3]),
                            count(when((col("Product_ID").contains(productidColumn[4])), True)).alias('c'+productidColumn[4]),
                            count(when((col("Product_ID").contains(productidColumn[5])), True)).alias('c'+productidColumn[5]),
                            count(when((col("Product_ID").contains(productidColumn[6])), True)).alias('c'+productidColumn[6]))
                       .orderBy(groupColumn))
# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoItemGasDieselYMcProductid"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMcProductid.write.format('json').save(outputFull)

# 群組欄位
groupColumn = ['Deptno', 'dateYear']
# 加油站－年》計算汽油及柴油產品筆數
deptnoYcProductid = (tDf
                       .groupBy(groupColumn)
                       .agg(count(when((col("Product_ID").contains(productidColumn[0])), True)).alias('c'+productidColumn[0]),
                            count(when((col("Product_ID").contains(productidColumn[1])), True)).alias('c'+productidColumn[1]),
                            count(when((col("Product_ID").contains(productidColumn[2])), True)).alias('c'+productidColumn[2]),
                            count(when((col("Product_ID").contains(productidColumn[3])), True)).alias('c'+productidColumn[3]),
                            count(when((col("Product_ID").contains(productidColumn[4])), True)).alias('c'+productidColumn[4]),
                            count(when((col("Product_ID").contains(productidColumn[5])), True)).alias('c'+productidColumn[5]),
                            count(when((col("Product_ID").contains(productidColumn[6])), True)).alias('c'+productidColumn[6]))
                       .orderBy(groupColumn))
# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoItemGasDieselYcProductid"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYcProductid.write.format('json').save(outputFull)

######

############

# 產品
productidColumn = ['113F 1209800', '113F 1209500', '113F 1209200', '113F 1229500',
                   '113F 5100100',
                   '113F 5100700', '113F 5100800']
# 表列要統計的欄位名稱
statColumn = ['Deptno', 'Tran_Time', 'Product_ID']
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
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay']
# 加油站－年－月－日》計算汽油及柴油產品筆數
deptnoYMDcProductid = (tDf
                       .groupBy(groupColumn)
                       .agg(count(when((col("Product_ID").contains(productidColumn[0])), True)).alias('c'+productidColumn[0]),
                            count(when((col("Product_ID").contains(productidColumn[1])), True)).alias('c'+productidColumn[1]),
                            count(when((col("Product_ID").contains(productidColumn[2])), True)).alias('c'+productidColumn[2]),
                            count(when((col("Product_ID").contains(productidColumn[3])), True)).alias('c'+productidColumn[3]),
                            count(when((col("Product_ID").contains(productidColumn[4])), True)).alias('c'+productidColumn[4]),
                            count(when((col("Product_ID").contains(productidColumn[5])), True)).alias('c'+productidColumn[5]),
                            count(when((col("Product_ID").contains(productidColumn[6])), True)).alias('c'+productidColumn[6]))
                       .orderBy(groupColumn))
# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "cityItemGasDieselYMDcProductid"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDcProductid.write.format('json').save(outputFull)

############


# 3.2 汽、機車加油筆數：加油站-年-月-日-車種（Amt > 249 | Amt < 250）-筆數
# 產品
productidColumn = ['113F 1209800', '113F 1209500', '113F 1209200', '113F 1229500',
                   '113F 5100100',
                   '113F 5100700', '113F 5100800']
# 取出符合產品項目的資料記錄
fDf = df.where(df.Product_ID.contains(productidColumn[0]) |
               df.Product_ID.contains(productidColumn[1]) |
               df.Product_ID.contains(productidColumn[2]) |
               df.Product_ID.contains(productidColumn[3]) |
               df.Product_ID.contains(productidColumn[4]) |
               df.Product_ID.contains(productidColumn[5]) |
               df.Product_ID.contains(productidColumn[6]))
# 表列要統計的欄位名稱
statColumn = ['Deptno', 'Tran_Time', 'Amt']
# 取出特定欄位
pDf = fDf.select(statColumn)
# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn('dateYear', pDf['Tran_Time'].substr(1, 4))
       .withColumn('dateMonth', pDf['Tran_Time'].substr(6, 2))
       .withColumn('dateDay', pDf['Tran_Time'].substr(9, 2)))
# 刪除不必要欄位
tDf = tDf.drop(tDf.Tran_Time)

# 群組欄位
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay']
# 加油站－年－月－日》計算交易金額在（1）小於等於249及（2）大於等於250的筆數
deptnoYMDcAmt = (tDf
                 .groupBy(groupColumn)
                 .agg(count(when((col("Amt").cast('float') < 250), True)).alias('cBike'),
                      count(when((col("Amt").cast('float') >= 250), True)).alias('cCar'))
                 .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoItemGasDieselYMDcAmt"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDcAmt.write.format('json').save(outputFull)

######

# 群組欄位
groupColumn = ['Deptno', 'dateYear', 'dateMonth']
# 加油站－年－月》計算交易金額在（1）小於等於249及（2）大於等於250的筆數
deptnoYMcAmt = (tDf
                 .groupBy(groupColumn)
                 .agg(count(when((col("Amt").cast('float') < 250), True)).alias('cBike'),
                      count(when((col("Amt").cast('float') >= 250), True)).alias('cCar'))
                 .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoYMcAmt"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMcAmt.write.format('json').save(outputFull)

# 群組欄位
groupColumn = ['Deptno', 'dateYear']
# 加油站－年》計算交易金額在（1）小於等於249及（2）大於等於250的筆數
deptnoYcAmt = (tDf
                 .groupBy(groupColumn)
                 .agg(count(when((col("Amt").cast('float') < 250), True)).alias('cBike'),
                      count(when((col("Amt").cast('float') >= 250), True)).alias('cCar'))
                 .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoItemGasDieselYcAmt"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYcAmt.write.format('json').save(outputFull)

######

# 3.3 加油站的發油量：加油站-年-月-日-發油量
# 產品
productidColumn = ['113F 1209800', '113F 1209500', '113F 1209200', '113F 1229500',
                   '113F 5100100',
                   '113F 5100700', '113F 5100800']
# 取出符合產品項目的資料記錄
fDf = df.where(df.Product_ID.contains(productidColumn[0]) |
               df.Product_ID.contains(productidColumn[1]) |
               df.Product_ID.contains(productidColumn[2]) |
               df.Product_ID.contains(productidColumn[3]) |
               df.Product_ID.contains(productidColumn[4]) |
               df.Product_ID.contains(productidColumn[5]) |
               df.Product_ID.contains(productidColumn[6]))
# 表列要統計的欄位名稱
statColumn = ['Deptno', 'Tran_Time', 'Qty']
# 取出特定欄位
pDf = fDf.select(statColumn)
# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn('dateYear', pDf['Tran_Time'].substr(1, 4))
       .withColumn('dateMonth', pDf['Tran_Time'].substr(6, 2))
       .withColumn('dateDay', pDf['Tran_Time'].substr(9, 2)))
# 刪除不必要欄位
tDf = tDf.drop(tDf.Tran_Time)

# 群組欄位
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay']
# 加油站－年－月－日》計算交易金額在（1）小於等於249及（2）大於等於250的筆數
deptnoYMDsQty = (tDf
                 .groupBy(groupColumn)
                 .agg(sum(tDf.Qty.cast('float')).alias('sQty'))
                 .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoItemGasDieselYMDsQty"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDsQty.write.format('json').save(outputFull)

######

# 群組欄位
groupColumn = ['Deptno', 'dateYear', 'dateMonth']
# 加油站－年－月》計算交易金額在（1）小於等於249及（2）大於等於250的筆數
deptnoYMDsQty = (tDf
                 .groupBy(groupColumn)
                 .agg(sum(tDf.Qty.cast('float')).alias('sQty'))
                 .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoItemGasDieselYMsQty"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDsQty.write.format('json').save(outputFull)

# 群組欄位
groupColumn = ['Deptno', 'dateYear']
# 加油站－年》計算交易金額在（1）小於等於249及（2）大於等於250的筆數
deptnoYMDsQty = (tDf
                 .groupBy(groupColumn)
                 .agg(sum(tDf.Qty.cast('float')).alias('sQty'))
                 .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoItemGasDieselYsQty"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDsQty.write.format('json').save(outputFull)
######


# 3.4 加油站的發油量：加油站－年-月-日-各項油品發油量總數

# 產品
productidColumn = ['113F 1209800', '113F 1209500', '113F 1209200', '113F 1229500',
                   '113F 5100100',
                   '113F 5100700', '113F 5100800']
# 取出符合產品項目的資料記錄
fDf = df.where(df.Product_ID.contains(productidColumn[0]) |
               df.Product_ID.contains(productidColumn[1]) |
               df.Product_ID.contains(productidColumn[2]) |
               df.Product_ID.contains(productidColumn[3]) |
               df.Product_ID.contains(productidColumn[4]) |
               df.Product_ID.contains(productidColumn[5]) |
               df.Product_ID.contains(productidColumn[6]))
# 表列要統計的欄位名稱
statColumn = ['Deptno', 'Tran_Time', 'Qty', 'Product_ID']
# 取出特定欄位
pDf = fDf.select(statColumn)
# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn('dateYear', pDf['Tran_Time'].substr(1, 4))
       .withColumn('dateMonth', pDf['Tran_Time'].substr(6, 2))
       .withColumn('dateDay', pDf['Tran_Time'].substr(9, 2)))
# 刪除不必要欄位
tDf = tDf.drop(tDf.Tran_Time)

# 群組欄位
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay']
# 加油站－年－月－日》各項油品發油量總數
deptnoYMDsQty = (tDf
                 .groupBy(groupColumn)
                 .agg(sum(when((col("Product_ID").contains(productidColumn[0])),
                               tDf.Qty.cast('float'))).alias('s'+productidColumn[0]),
                      sum(when((col("Product_ID").contains(productidColumn[1])),
                               tDf.Qty.cast('float'))).alias('s'+productidColumn[1]),
                      sum(when((col("Product_ID").contains(productidColumn[2])),
                               tDf.Qty.cast('float'))).alias('s'+productidColumn[2]),
                      sum(when((col("Product_ID").contains(productidColumn[3])),
                               tDf.Qty.cast('float'))).alias('s'+productidColumn[3]),
                      sum(when((col("Product_ID").contains(productidColumn[4])),
                               tDf.Qty.cast('float'))).alias('s'+productidColumn[4]),
                      sum(when((col("Product_ID").contains(productidColumn[5])),
                               tDf.Qty.cast('float'))).alias('s'+productidColumn[5]),
                      sum(when((col("Product_ID").contains(productidColumn[6])),
                               tDf.Qty.cast('float'))).alias('s'+productidColumn[6]))
                 .orderBy(groupColumn))                 

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "deptnoItemGasDieselYMDsQty"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYMDsQty.write.format('json').save(outputFull)

