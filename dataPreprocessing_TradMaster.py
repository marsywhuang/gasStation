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
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 來源路徑
inputPath = "/home/mywh/data/resultData/tranMaster_20170*/tranMaster_20170*.csv"
# 來源資料
inputFile = "p*"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile

# 讀入來源資料
df = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "false")

# 表列要統計的欄位名稱
statColumn = ['_c0', '_c3', '_c16']
# 取出特定欄位
pDf = df.select(statColumn)
pDf = (pDf
       .withColumnRenamed('_c0', 'StdNo')
       .withColumnRenamed('_c3', 'Date')
       .withColumnRenamed('_c16', 'Payment'))
#
tDf = (pDf
       .withColumn('dateYear', pDf['Date'].substr(1, 4))
       .withColumn('dateMonth', pDf['Date'].substr(6, 2))
       .withColumn('dateDay', pDf['Date'].substr(9, 2))
      )
#
groupColumn = ['StdNo', 'Date', 'Payment', 'DateYear', 'DateMonth', 'DateDay']
paymentColumn = ['900', '901', '902', '903', '905', '906', '907', '931', '933']
#
stdnoPaymentYearDf = (tDf
                      .groupBy(groupColumn[0], groupColumn[2], groupColumn[4], groupColumn[5])
                      .agg(count(tDf.Payment.alias('aPayment')))
                      .orderBy(groupColumn[0], groupColumn[2], groupColumn[4], groupColumn[5]))
#
stdnoPaymentYearMonthDf = (tDf
                      .groupBy(groupColumn[0], groupColumn[2], groupColumn[3])
                      .agg(count(tDf.Payment.alias('aPayment')))
                      .orderBy(groupColumn[0], groupColumn[2], groupColumn[3]))
#
paymentStdnoYearDf = (tDf
                      .groupBy(groupColumn[2], groupColumn[0], groupColumn[3])
                      .agg(count(tDf.Payment.alias('aPayment')))
                      .orderBy(groupColumn[2], groupColumn[0], groupColumn[3]))
