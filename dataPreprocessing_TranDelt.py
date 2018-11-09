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
inputPath = "/home/cpc/data/resultData/tranDelt/tranDelt_*/tranDelt_*/tranDelt_*.csv"
# 來源資料
inputFile = "p*"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile

# 讀入來源資料
df = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "false")

# tran_delt 欄位名稱
# _c0 Deptno, _c1 Island, _c2 Gun_No, _c3 Tran_Time, _c4 Seq,
# _c5 Tax_Type, _c6 Product_ID, _c7 Class, _c8 Price, _c9 Amt,
# _c10 Qty, _c11 Unit, _c12 Ref_No, _c13 Shift

#
# 加油站－年－月－日》加總（量）
#

# 表列要統計的欄位名稱
# _c0 Deptno, _c3 Date, _c10 Qty
statColumn = ['_c0', '_c3', '_c10']

# 取出特定欄位
pDf = df.select(statColumn)
pDf = (pDf
       .withColumnRenamed('_c0', 'Deptno')
       .withColumnRenamed('_c3', 'Date')
       .withColumnRenamed('_c10', 'Qty'))

# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn('dateYear', pDf['Date'].substr(1, 4))
       .withColumn('dateMonth', pDf['Date'].substr(6, 2))
       .withColumn('dateDay', pDf['Date'].substr(9, 2)))

# 刪除不必要欄位
tDf = tDf.drop(tDf.Date)

# 群組欄位
groupColumn = ['Deptno', 'dateYear', 'dateMonth', 'dateDay']

# 加油站－年－月－日》加總（量）
stdnoPaymentYearMonthDayDf = (tDf
                              .groupBy(groupColumn)
                              .agg(sum(tDf.Qty.cast('float')).alias('aQty'))
                              .orderBy(groupColumn))


# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "tranDelt_DeptnoPaymentYearMonthDay_sQty.json"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
stdnoPaymentYearDf.write.format('json').save(outputFull)

#
# 加油站－產品－年－月－日》計數（筆數）
#

# 表列要統計的欄位名稱
# _c0 Deptno, _c3 Tran_Time, _c6 Product_ID
statColumn = ['_c0', '_c3', '_c6']

# 取出特定欄位
pDf = df.select(statColumn)
pDf = (pDf
       .withColumnRenamed('_c0', 'Deptno')
       .withColumnRenamed('_c3', 'Date')
       .withColumnRenamed('_c6', 'ProductId'))

# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn('dateYear', pDf['Date'].substr(1, 4))
       .withColumn('dateMonth', pDf['Date'].substr(6, 2))
       .withColumn('dateDay', pDf['Date'].substr(9, 2)))

# 刪除不必要欄位
tDf = tDf.drop(tDf.Date)

# 群組欄位
groupColumn = ['Deptno', 'ProductId', 'dateYear', 'dateMonth', 'dateDay']

# 加油站－產品－年－月－日》計數（筆數）
deptnoProductidYearMonthDay = (tDf
                              .groupBy(groupColumn)
                              .agg(count(tDf.ProductId).alias('aProductId'))
                              .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "tranDelt_DeptnoProductidYearMonthDay_cProductID.json"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoProductidYearMonthDayDf.write.format('json').save(outputFull)

#
# 加油站－金額（小於等於249）－年－月－日》計數（筆數）
# 加油站－金額（大於250）－年－月－日》計數（筆數）
#

# 表列要統計的欄位名稱
# _c0 Deptno, _c3 Date, _c9 Amt
statColumn = ['_c0', '_c3', '_c9']

# 取出特定欄位
pDf = df.select(statColumn)
pDf = (pDf
       .withColumnRenamed('_c0', 'Deptno')
       .withColumnRenamed('_c3', 'Date')
       .withColumnRenamed('_c9', 'Amt'))

# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn('dateYear', pDf['Date'].substr(1, 4))
       .withColumn('dateMonth', pDf['Date'].substr(6, 2))
       .withColumn('dateDay', pDf['Date'].substr(9, 2)))

# 刪除不必要欄位
tDf = tDf.drop(tDf.Date)

#
deptnoYearMonthDay = (tDf
                      .groupBy(groupColumn)
                      .agg(count(when((col("Amt").cast('float') < 250), True)).alias('aBike'),
                           count(when((col("Amt").cast('float') >= 250), True)).alias('aCar'))
                      .orderBy(groupColumn))

# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "tranDelt_deptnoYearMonthDay_cAmt.json"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
deptnoYearMonthDay.write.format('json').save(outputFull)
