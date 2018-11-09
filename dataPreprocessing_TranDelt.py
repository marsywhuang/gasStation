# 載入環境
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession

# 載入函式庫
from pyspark.sql.functions import sum
from pyspark.sql.functions import count

# 來源路徑
inputPath = "/home/cpc/data/resultData/tranDelt/tranDelt_*/tranDelt_*/tranDelt_*.csv"
# 來源資料
inputFile = "p*"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile

# 讀入來源資料
df = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "false")

# tran_delt 欄位名稱
# Deptno, Island, Gun_No, Tran_Time, Seq, Tax_Type, Product_ID, Class, Price, Amt,
# Qty, Unit, Ref_No, Shift


# 表列要統計的欄位名稱
# _c0 StdNo, _c3 Date, _c9 Qty
statColumn = ['_c0', '_c3', '_c9']

# 取出特定欄位
pDf = df.select(statColumn)
pDf = (pDf
       .withColumnRenamed('_c0', 'StdNo')
       .withColumnRenamed('_c3', 'Date')
       .withColumnRenamed('_c9', 'Qty'))

# 轉換日期欄位成為年、月及日等三個欄位
tDf = (pDf
       .withColumn('dateYear', pDf['Date'].substr(1, 4))
       .withColumn('dateMonth', pDf['Date'].substr(6, 2))
       .withColumn('dateDay', pDf['Date'].substr(9, 2)))

# 刪除不必要欄位
tDf = tDf.drop(tDf.Date)

# 群組欄位
groupColumn = ['StdNo', 'dateYear', 'dateMonth', 'dateDay']

# 加油站－年－月－日》加總（量）
stdnoPaymentYearMonthDayDf = (tDf
                              .groupBy(groupColumn)
                              .agg(sum(tDf.Qty.cast('float')).alias('aQty'))
                              .orderBy(groupColumn))


# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的檔案名稱
outputFile = "tranDeltStdnoPaymentYearMonthDaySum.json"
# 完整路徑和名稱
outputFull = outputPath + "/" + outputFile
# 匯出資料
stdnoPaymentYearDf.write.format('json').save(outputFull)

# 加油站－產品－年－月－日》計數（筆數）

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

# 加油站－年－月－日》加總（量）
deptnoProductidYearMonthDayDf = (tDf
                              .groupBy(groupColumn)
                              .agg(count(tDf.ProductId).alias('aProductId'))
                              .orderBy(groupColumn))

# 加油站－金額（小於等於249）－年－月－日》計數（筆數）

# 加油站－金額（大於250）－年－月－日》計數（筆數）

