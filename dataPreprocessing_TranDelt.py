# 載入環境
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession

# 載入函式庫
from pyspark.sql.functions import count
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import sum
from pyspark.sql.functions import desc

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
#
tDf = (pDf
       .withColumn('dateYear', pDf['Date'].substr(1, 4))
       .withColumn('dateMonth', pDf['Date'].substr(6, 2))
       .withColumn('dateDay', pDf['Date'].substr(9, 2)))
#
groupColumn = ['StdNo', 'Date', 'Qty', 'dateYear', 'dateMonth', 'dateDay']
#
stdnoPaymentYearDf = (tDf
                      .groupBy(groupColumn[0], groupColumn[2], groupColumn[3], groupColumn[4], groupColumn[5])
                      .agg(sum(tDf.Qty.cast('float')).alias('aQty'))
                      .orderBy(groupColumn[0], groupColumn[2], groupColumn[3], groupColumn[4], groupColumn[5]))


# 路徑
outputPath = "/home/cpc/data/resultData"
# 資料
outputFile = "tranDeltStdnoPaymentYearMonthDaySum.json"
# 完整路徑和資料
outputFull = outputPath + "/" + outputFile
#
stdnoPaymentYearDf.write.format('json').save(outputFull)
