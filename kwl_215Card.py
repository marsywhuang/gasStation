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

#
# 加油站服務類型站數統計表
#

# 來源路徑
inputPath = "/home/mywh/data/rawData"
# 來源資料
inputFile = "infoCpcGasStation.csv"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile

# 讀入來源資料
df = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "true")

# 表列要統計的欄位名稱
statColumn = ['類別', '縣市', '服務中心', '營業中', '國道高速公路',
              '無鉛92', '無鉛95', '無鉛98', '酒精汽油', '煤油', '超柴',
              '會員卡', '刷卡自助', '自助柴油站', '電子發票', '悠遊卡', '一卡通', 'HappyCash',
              '洗車類別']

# 
for idxCol in range(len(df.columns)):
  # 判斷是否要進行分類計數
  if (df.columns[idxCol] in statColumn):
    #
    df.columns[idxCol]
    # df.select(df[idxCol]).distinct().count()
    # 列出所有記錄
    for idxRow in df.groupBy(df.columns[idxCol]).agg(count(df.columns[idxCol])).collect():
      idxRow

#
# 車隊卡－坤神
#

# 來源路徑
inputPathL1 = "/home/cpc/data/resultData"
inputPathL2 = "215Card/215Card_*/215Card_*/215Card_*"
inputPath = inputPathL1 + "/" + inputPathL2
# 來源資料
inputFile = "p*"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile

# 讀入來源資料
df = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "true")

# 油品項目
# 113F 1209800	98無鉛汽油	
# 113F 1209500	95無鉛汽油
# 113F 1209200	92無鉛汽油	
# 113F 1229500	酒精汽油
# 113F 5100100	超級柴油	
# 113F 5100700	海運輕柴油	
# 113F 5100800  海運重柴油
productColumn = ['113F 1209800', '113F 1209500', '113F 1209200',
                 '113F 1229500',
                 '113F 5100100',
                 '113F 5100700' , '113F 5100800']


#
# 車隊-車號-年-月-日》加總（里程數）、加總（加油量）、計次（加油量）
#

# 取出特定欄位
statColumn = ['CUSAUNT', 'CARNO',  'STDNO', 'TDATE', 'QTY', 'MILE']
pDf = df.select(statColumn)
# 分離日期欄位的年及月至新欄位
tDf = (pDf
       .withColumn('TDATEYEAR', pDf['TDATE'].substr(1, 4))
       .withColumn('TDATEMONTH', pDf['TDATE'].substr(5, 2))
       .withColumn('TDATEDAY', pDf['TDATE'].substr(7, 2)))
# 刪除不必要欄位
tDf = tDf.drop('TDATE')
#
groupColumn = ["CUSAUNT", "CARNO", "STDNO", "TDATEYEAR", "TDATEMONTH", "TDATEDAY"]
sDf = (tDf
       .groupBy(groupColumn)
       .agg(
         sum(tDf.QTY.cast("float")).alias("sQty"),
         sum(tDf.MILE.cast("float")).alias("sMile"),
         count(tDf.QTY.cast("float")).alias("cTimes"))
       .orderBy(groupColumn))
#
# 目的路徑
outputPath = "/home/cpc/data/resultData"
# 目的資料
outputFile = "cusauntCarnoYMDsQtysMilecTimes"
# 完整路徑和資料
outputFull = outputPath + "/" + outputFile
# 儲存結果
sDf.toJSON().coalesce(1).saveAsTextFile(outputFull)
