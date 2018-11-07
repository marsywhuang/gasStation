# 載入函式庫
from pyspark.sql.functions import count
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import sum

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
# 年度月油品（汽油/柴油）銷售總量
#

# 來源路徑
inputPath = "/home/mywh/data/rawData"
# 來源資料
inputFile = "215Card.csv"
# 完整路徑和資料
inputFull = inputPath + "/" + inputFile
# 讀入來源資料
df215Card = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "true")

#
# 年度月油品（汽油/柴油）銷售總量
#

# 表列要統計的欄位名稱
statColumn = ['PNO', 'TDATE', 'QTY']
# 取出特定欄位
pDf215Card = df215Card.select(statColumn)
#
tDf215Card = (pDf215Card.withColumn('TDATEYEAR', pDf215Card['TDATE'].substr(1, 4))
                        .withColumn('TDATEMONTH', pDf215Card['TDATE'].substr(5, 2)))

# 群組欄位
groupColumn = ['TDATEYEAR', 'TDATEMONTH', 'PNO']
# 根據 年、月、油品 欄位，計算 某年某月特定油品的總銷量
for idxRow in tDf215Card.groupBy(groupColumn).agg(sum(tDf215Card.QTY.cast('float'))).orderBy(groupColumn).collect():
  idxRow

#
# 同期｛全部｜汽油｜柴油｝銷售總量
#

# 群組欄位
groupColumn = ['TDATEYEAR', 'TDATEMONTH']
# 根據｛年｝｛月｝［油品］欄位，計算 計算［全部］的總銷量
for idxRow in tDf215Card.groupBy(groupColumn).agg(sum(tDf215Card.QTY.cast('float'))).orderBy(groupColumn).collect():
  idxRow
# 根據｛年｝｛月｝欄位，計算［汽油］的總銷量
# 根據｛年｝｛月｝欄位，計算［柴油］的總銷量

#
# 去年度/本年度同期銷量差異
#

# 表列要統計的欄位名稱
statColumn = ['CUSAUNT', 'PNO', 'TDATE', 'QTY']
# 取出特定欄位
pDf215Card = df215Card.select(statColumn)
#
tDf215Card = (pDf215Card.withColumn('TDATEYEAR', pDf215Card['TDATE'].substr(1, 4))
                        .withColumn('TDATEMONTH', pDf215Card['TDATE'].substr(5, 2)))

# 群組欄位
groupColumn = ['CUSAUNT', 'TDATEYEAR', 'TDATEMONTH', 'PNO']
# 根據｛企業客戶｝｛年｝｛月｝欄位，計算［汽油］的總銷量
for idxRow in tDf215Card.groupBy(groupColumn).agg(sum(tDf215Card.QTY.cast('float'))).orderBy(groupColumn).collect():
  idxRow
  
#
# 去年度/本年度同期累計銷量差異
#

#
# 銷量佔所有企業客戶比例
#

#
# 年度自營站與加盟站銷量比重
#

#
# 銷量佔比前10大加油站
#

# 表列要統計的欄位名稱
statColumn = ['CUSAUNT', 'STDNO', 'TDATE', 'QTY']
# 取出特定欄位
pDf215Card = df215Card.select(statColumn)
#
tDf215Card = (pDf215Card.withColumn('TDATEYEAR', pDf215Card['TDATE'].substr(1, 4))
                        .withColumn('TDATEMONTH', pDf215Card['TDATE'].substr(5, 2)))

# 群組欄位
groupColumn = ['CUSAUNT', 'STDNO', 'TDATEYEAR', 'TDATEMONTH']
# 根據｛企業客戶｝｛加油站代號｝｛年｝｛月｝欄位，計算［全部］的總銷量
for idxRow in tDf215Card.groupBy(groupColumn).agg(sum(tDf215Card.QTY.cast('float'))).orderBy(groupColumn).collect():
  idxRow

#
# 與去年同期車隊卡差異
#

#
# 未使用代碼
#

# 根據 年、月、油品 欄位，計算 某年某月特定油品 次數
for idxRow in tDf215Card.groupBy(groupColumn).agg(count('PNO')).orderBy(groupColumn).collect():
  idxRow
