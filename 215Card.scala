// 載入函式庫
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql

// 起動
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// 定義資料結構
val schema = ((new StructType)
  .add("RID", StringType)
  .add("CTYPE", StringType)
  .add("STDNO", StringType)
  .add("PNO", StringType)
  .add("UNT", StringType)
  .add("LDATE", StringType)
  .add("TDATE", StringType)
  .add("QTY", FloatType)
  .add("STRNO", StringType)
  .add("TNO", StringType)
  .add("MRK3", StringType)
  .add("MRK4", StringType)
  .add("MRK5", StringType)
  .add("TICKETNO", StringType)
  .add("SHIP", StringType)
  .add("CARNO", StringType)
  .add("PNAME", StringType)
  .add("TICKETTYPE", StringType)
  .add("CUSAUNT", StringType)
  .add("CUSMUNT", StringType)
  .add("TTIME", StringType)
  .add("CARDMNO", StringType)
  .add("CTYPEMK", StringType)
  .add("MILE", StringType)
  .add("PRICE", StringType)
  .add("CNO", StringType)
  .add("BILLNO", StringType)
  .add("ATYPE", StringType)
  .add("ADATE", StringType)
  .add("RDATE", StringType)
  .add("MDATE", StringType)
  .add("SID", StringType)
  .add("YYMM", StringType)
  .add("BPRICE", StringType)
  .add("SPRICE", StringType)
  .add("MK1", StringType)
  .add("MK2", StringType)
  .add("MK3", StringType)
  .add("S3_SEQNO", StringType)
  .add("ISLAND_NO", StringType)
  .add("GUN_NO", StringType)
  .add("EDC_VERSION", StringType))

//
val schema = StructType(Array(
  StructField("RID", StringType, true),
  StructField("CType", StringType, true),
  StructField("StdNo", StringType, true),
  StructField("PNo", StringType, true),
  StructField("Unt", StringType, true),
  StructField("LDate", StringType, true),
  StructField("TDate", TimestampType, true),
  StructField("Qty", FloatType, true),
  StructField("StrNo", StringType, true),
  StructField("TNo", StringType, true),
  StructField("Mrk3", StringType, true),
  StructField("Mrk4", StringType, true),
  StructField("Mrk5", StringType, true),
  StructField("TicketNo", StringType, true),
  StructField("Ship", StringType, true),
  StructField("CarNo", StringType, true),
  StructField("PName", StringType, true),
  StructField("TicketType", StringType, true),
  StructField("CusAUnt", StringType, true),
  StructField("CusMUnt", StringType, true),
  StructField("TTime", StringType, true),
  StructField("CardMNo", StringType, true),
  StructField("CTypeMk", StringType, true),
  StructField("Mile", StringType, true),
  StructField("Price", StringType, true),
  StructField("CNo", StringType, true),
  StructField("BillNo", StringType, true),
  StructField("AType", StringType, true),
  StructField("ADate", StringType, true),
  StructField("rDate", StringType, true),
  StructField("MDate", StringType, true),
  StructField("SID", StringType, true),
  StructField("YYMM", StringType, true),
  StructField("Bprice", StringType, true),
  StructField("Sprice", StringType, true),
  StructField("Mk1", StringType, true),
  StructField("Mk2", StringType, true),
  StructField("Mk3", StringType, true),
  StructField("S3_SEQNO", StringType, true),
  StructField("ISLAND_SEQNO", StringType, true),
  StructField("EDC_VERSION", StringType, true)
))

// 來源目錄名稱
val inputPath = "/home/mywh/data"
// 來源檔案名稱
val inputFileName = "215Card.csv"
val inputFileName = "Ubus215.csv"
// 整體目錄及檔案名稱
val inputFull = inputPath + "/" + inputFileName

// 讀入檔案
val df = sqlContext.read.format("csv").option("header", "true").option("schema", "schema").load(inputFull)

// 取出 加油站代號、交易日期、量、車號、企業客戶代號、哩程
val pDf = df.select("StdNo", "TDate", "Qty", "CarNo", "CusAUnt", "Mile")


// 根據 CUSAUNT、CARNO 及 STDNO 進行分群
val gDf = df.groupBy("CUSAUNT", "CARNO", "STDNO").agg(collect_list("TDATE").alias("TDATE"), collect_list("QTY").alias("QTY")).sort("CUSAUNT", "CARNO", "STDNO")

val gDf = df.groupBy("CUSAUNT", "CARNO", "STDNO").agg(collect_list(struct("TDATE", "QTY", "MILE")).alias("Message")).sort("CUSAUNT", "CARNO", "STDNO")

// 來源目錄名稱
val outputPath = "/home/mywh/data"
// 來源檔案名稱
val outputFileName = "215CardGroup.parquet"
// 整體目錄及檔案名稱
val outputFull = outputPath + "/" + outputFileName

// 以 parque 格式存檔
gDf.write.parquet(outputFull)

// 來源檔案名稱
val outputFileName = "215CardGroup.json"
// 整體目錄及檔案名稱
val outputFull = outputPath + "/" + outputFileName
// 以 json 格式存檔
gDf.write.json(outputFull)


// 更改 Qty 欄位屬性成為 float 型別
val tDf = df.withColumn("Qty", df("Qty").cast(sql.types.FloatType))

// 針對 客戶編號、車號 及 加油站代號 進行小計
val rDf = tDf.rollup("CusAUnt", "CarNo", "StdNo").agg(sum("Qty") as "aQty").select("CusAUnt", "CarNo", "StdNo", "aQty")
rDf.orderBy("CusAUnt", "CarNo", "StdNo").collect().foreach(println)

// 根據 車號 及 加油站代號 計算總加油量
val rDf = tDf.rollup("CarNo", "StdNo").agg(sum("Qty") as "aQty").select("CarNo", "StdNo", "aQty")
rDf.orderBy("CarNo", "StdNo").collect().foreach(println)

// 根據 客戶編號 計算總加油量
val rDf = tDf.rollup("CusAUnt").agg(sum("Qty") as "aQty").select("CusAUnt", "aQty")
rDf.orderBy("CusAUnt").collect().foreach(println)

// 根據 車號 計算總加油量
val rDf = tDf.rollup("CarNo").agg(sum("Qty") as "aQty").select("CarNo", "aQty")
rDf.orderBy("CarNo").collect().foreach(println)

// 根據 加油站代號 計算總加油量
val rDf = tDf.rollup("StdNo").agg(sum("Qty") as "aQty").select("StdNo", "aQty")
rDf.orderBy("StdNo").collect().foreach(println)

//
val tmpGroupDf = gDf.toDF.persist()
// 顯示每筆記錄
for (idxItm <- tmpGroupDf.collect()) {
  println(idxItm)
}



//
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// 來源目錄名稱
val inputPath = "/home/mywh/data"
// 來源檔案名稱
val inputFileName = "215CardGroup.parquet"
// 整體目錄及檔案名稱
val inputFull = inputPath + "/" + inputFileName
//
val df = sqlContext.read.parquet(inputFull)
