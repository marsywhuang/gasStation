# 載入函式庫
from pyspark.sql.functions import count
from pyspark.sql.functions import countDistinct

# 來源路徑
inputPath = "/home/mywh/data/rawData"
# 來源資料
inputFile = "infoCpcGasStation.csv"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile

# 讀入來源資料
df = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "true")

# 
for idxCol in range(len(df.columns)):
  df.columns[idxCol]
  # df.select(df[idxCol]).distinct().count()
  for idxRow in df.groupBy(df.columns[idxCol]).agg(count(df.columns[idxCol])).collect():
    idxRow
