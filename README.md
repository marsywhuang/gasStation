# gasStation

// 載入函式庫

import org.apache.spark.sql

// 起動

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// 定義資料結構

// 註：若可得知載入資料的欄位及其屬性，建議將欄位及其屬性藉由變數方式先行定義
// 如此一來，spark 不用逐行確定屬性，資料載入快
// 註：若可事先判斷欄位的屬性，建議事先指定，可節省儲存空間；若未指定情況，spark 預設成 string 格式
val schema = ((new StructType)
  .add("Column0", StringType)
  .add("Column1", FloatType)
  ...)

// 讀入具 CVS 格式的檔案、設定第一筆資料為欄位名稱、套用預設格式(欄位名稱、屬性)
val df = sqlContext.read.format("csv").option("header", "true").option("schema", "schema").load(inputFull)

// Column1 欄位屬性成為 date 型別，以及更改 Column8 欄位屬性成為 float 型別
// 註：date 型別的格式設定，一定要與欄位內容表達方式相符，
// 例如：20181020 就要表示成 yyyyMMdd、2018-10-20 就要表示成 yyyy-MM-dd
val tDf = (df.withColumn("Column1", to_date("Column1", "yyyyMMdd")).
  withColumn("Column8", df("Column8").cast(sql.types.FloatType)))

// 取出 欄位1、欄位3、欄位7、欄位8、欄位10、欄位17
// 註：操作資料時，並不需要將所有欄位全數帶入，可節省記憶體和暫存空間
// 註：下列命令列僅表示 pDf 來自於 df，此時，未將 df 資料實際轉換至 pDf，因為 select 是一個 transformation 型指令
val pDf = df.select("Column1", "Column3", "Column7", "Column8", "Column10", "Column17")

// 通過 groupBy 指令，以欄位1、欄位3及欄位7作為特徵，聚合欄位8、欄位10及欄位17
// 欄位8、欄位10及欄位17的結果有二種表示方式
// 結果1：(欄位1、欄位3、欄位7)》(欄位8、欄位10、欄位17)
// 結果2：(欄位1、欄位3、欄位7)》(欄位8)、(欄位10)、(欄位17)
// 結果1是將欄位8、欄位10及欄位17等3個欄位視為一體，而結果2則是將欄位8、欄位10及欄位17等3個欄位分別視為各自獨立個體

// 根據欄位1、欄位3及欄位7進行分群，並將欄位8、欄位10及欄位17併入 Message 欄位，作為資料項目
val gDf = (df.groupBy("Column1", "Column3", "Column7").
  agg(collect_list(struct("Column8", "Column10", "Column17")).alias("Message")).
  sort("Column1", "Column3", "Column7"))

// 根據欄位1、欄位3及欄位7進行分群，欄位8、欄位10及欄位17分別以欄位8、欄位10及欄位17等欄位，作為資料項目
val gDf = (df.groupBy("Column1", "Column3", "Column7").
  agg(collect_list("Column8").alias("Column8"), collect_list("Column10").alias("Column10"), collect_list("Column17").alias("Column17")).
  sort("Column1", "Column3", "Column7"))

// 通過 groupBy 指令所得 gDf 可存成 parque 或 json 格式，不能使用 csv 格式，因為結果1或2的表示方式是一對多關係

// 以 parque 格式存檔
gDf.write.parquet(outputFull)

// 以 json 格式存檔
gDf.write.json(outputFull)
