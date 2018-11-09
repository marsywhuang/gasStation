# 載入環境
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession

# 載入函式庫
from pyspark.sql.functions import sum

# 來源路徑
inputPath = "/home/cpc/data/resultData/tranDelt/tranDelt_*/tranDelt_*/tranDelt_*.csv"
# 來源資料
inputFile = "p*"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile

# 讀入來源資料
df = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "false")

# 表列要統計的欄位名稱
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

#
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
