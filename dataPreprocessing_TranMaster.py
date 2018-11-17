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
inputPath = "/home/cpc/data/resultData/tranMaster/tranMaster_*/tranMaster_*/tranMaster_*.csv"

inputPath = "/home/cpc/data/tranMaster/tranMaster_2017/tranMaster_2017*/*.csv"
# 來源資料
inputFile = "p*"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile

# 讀入來源資料
df = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "true")

# 表列要統計的欄位名稱
statColumn = ['Deptno', 'Tran_time', 'Payment']

# 取出特定欄位
pDf = df.select(statColumn)

# 重製日期格式
tDf = (pDf
       .withColumn('dateYear', pDf['Tran_time'].substr(1, 4))
       .withColumn('dateMonth', pDf['Tran_time'].substr(6, 2))
       .withColumn('dateDay', pDf['Tran_time'].substr(9, 2)))

# 刪除不使用欄位
tDf = tDf.drop(tDf.Tran_time)

#
paymentColumn = ['900', '901', '902', '903', '905', '906', '907', '931', '933']

# 根據加油站、年、月及日，計算各類支付方式的使用次數
groupColumn = ['Deptno', 'Payment', 'dateYear', 'dateMonth']
deptnoYMDaPayment = (tDf
                     .groupBy(groupColumn)
                     .agg(count(tDf.Payment.alias('aPayment')))
                     .orderBy(groupColumn))

# 根據加油站、年、月及日，計算各類支付方式的使用次數
groupColumn = ['Deptno', 'Payment', 'dateYear', 'dateMonth', 'dateDay']
deptnoYMDaPayment = (tDf
                     .groupBy(groupColumn)
                     .agg(count(tDf.Payment.alias('aPayment')))
                     .orderBy(groupColumn))


# 路徑
outputPath = "/home/cpc/data/resultData"
# 資料
outputFile = "stdnoPaymentYearMonthDayCount.json"
# 完整路徑和資料
outputFull = outputPath + "/" + outputFile
#
deptnoPaymentYearDf.write.format('json').save(outputFull)
