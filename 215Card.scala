// 起動
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// 來源目錄名稱
val inputPath = "/home/mywh/data"
// 來源檔案名稱
val inputFileName = "215Card.csv"
// 整體目錄及檔案名稱
val inputFull = inputPath + "/" + inputFileName

// 讀入檔案
val df = sqlContext.read.format("csv").option("header", "true").load(inputFull)

// 根據 CUSAUNT、CARNO 及 STDNO 進行分群
val gDf = df.groupBy("CUSAUNT", "CARNO", "STDNO").agg(collect_list("TDATE").alias("TDATE"), collect_list("QTY").alias("QTY")).sort("CUSAUNT", "CARNO", "STDNO")

// 來源目錄名稱
val outputPath = "/home/mywh/data"

// 來源檔案名稱
val outputFileName = "215CardGroup.parquet"

// 整體目錄及檔案名稱
val outputFull = outputPath + "/" + outputFileName

// 以 parque 格式存檔
gDf.write.parquet(outputFull)

// 以 json 格式存檔
gDf.write.json("215CardGroup.json")


//
val tmpGroupDf = gDf.toDF.persist()
// 顯示每筆記錄
for (idxItm <- tmpGroupDf.collect()) {
  println(idxItm)
}
