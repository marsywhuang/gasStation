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
from pyspark.sql.functions import col
from pyspark.sql.functions import when

# 來源路徑
inputPath = "/home/cpc/data/tranMaster/tranMaster_*/tranMaster_*/tranMaster_*.csv"

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

# 支付方式
paymentColumn = ['900', '901', '902', '903', '905', '906', '907', '931', '933']

# 根據加油站、年及月，計算各類支付方式的使用次數
groupColumn = ['Deptno', 'dateYear', 'dateMonth']
deptnoYMaPayment = (tDf
                      .groupBy(groupColumn)
                      .agg(count(when((col("Payment") == paymentColumn[0]), True)).alias('a900'),
                           count(when((col("Payment") == paymentColumn[1]), True)).alias('a901'),
                           count(when((col("Payment") == paymentColumn[2]), True)).alias('a902'),
                           count(when((col("Payment") == paymentColumn[3]), True)).alias('a903'),
                           count(when((col("Payment") == paymentColumn[4]), True)).alias('a905'),
                           count(when((col("Payment") == paymentColumn[5]), True)).alias('a906'),
                           count(when((col("Payment") == paymentColumn[6]), True)).alias('a907'),
                           count(when((col("Payment") == paymentColumn[7]), True)).alias('a931'),
                           count(when((col("Payment") == paymentColumn[8]), True)).alias('a933'))
                      .orderBy(groupColumn))

# 路徑
outputPath = "/home/cpc/data/resultData"
# 資料
outputFile = "stdnoPaymentYearMonthDayCount.json"
# 完整路徑和資料
outputFull = outputPath + "/" + outputFile
#
deptnoPaymentYearDf.write.format('json').save(outputFull)
