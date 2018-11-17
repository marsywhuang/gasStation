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

# 來源路徑
inputPath = "/home/cpc/data/tranDelt/tranDelt_2017/tranDelt_*/tranDelt_*.csv"

# 來源資料
inputFile = "p*"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile

# 讀入來源資料
df = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "true")

#
# 加油站－年－月－日》加總（量）
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
# 加油站－年－月－日》加總（量）
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
# 加油站－產品－年－月－日》計數（筆數）
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
groupColumn = ['Deptno', 'ProductId', 'dateYear', 'dateMonth', 'dateDay']
# 加油站－產品－年－月－日》計數（筆數）
deptnoYMDaProductid = (tDf
                       .groupBy(groupColumn)
                       .agg(count(tDf.ProductId).alias('aProductId'))
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
#
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
groupColumn = ['Deptno', 'dateYear', 'dateMonth']
# 加油站－產品－年－月－日》計數（筆數）
deptnoYMaProductid = (tDf
                       .groupBy(groupColumn)
                       .agg(count(when((col("Product_ID").contains(productidColumn[0])), True)).alias('a'+productidColumn[0]),
                            count(when((col("Product_ID").contains(productidColumn[1])), True)).alias('a'+productidColumn[1]),
                            count(when((col("Product_ID").contains(productidColumn[2])), True)).alias('a'+productidColumn[2]),
                            count(when((col("Product_ID").contains(productidColumn[3])), True)).alias('a'+productidColumn[3]),
                            count(when((col("Product_ID").contains(productidColumn[4])), True)).alias('a'+productidColumn[4]),
                            count(when((col("Product_ID").contains(productidColumn[5])), True)).alias('a'+productidColumn[5]),
                            count(when((col("Product_ID").contains(productidColumn[6])), True)).alias('a'+productidColumn[6]))
                       .orderBy(groupColumn))

#
#
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
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay']
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
