// 載入函式庫
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql

// 起動
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// 來源目錄名稱
val inputPath = "/home/mywh/data/rawData"
// 來源檔案名稱
val inputFileName = "215Card.csv"
// 整體目錄及檔案名稱
val inputFull = inputPath + "/" + inputFileName

// 讀入檔案
val df = sqlContext.read.format("csv").option("header", "true").load(inputFull)

// 取出 加油站代號、交易日期、量、車號、企業客戶代號
val pDf = df.select("StdNo", "TDate", "Qty", "CarNo", "CusAUnt")

// TDate 欄位屬性成為 date 型別，以及更改 Qty 欄位屬性成為 float 型別
val tDf = (pDf.withColumn("TDate", to_date($"TDate", "yyyyMMdd")).
           withColumn("Qty", df("Qty").cast(sql.types.FloatType)))



// 日期級距
val dtYear = List(List[String]("2016", "2017", "2018"))
val dtFrom = List(List[String]("2017-01-01", "2017-02-01", "2017-03-01", "2017-04-01", "2017-05-01", "2017-06-01",
                               "2017-07-01", "2017-08-01", "2017-09-01", "2017-10-01", "2017-11-01", "2017-12-01"))
val dtTo = List(List[String]("2017-01-31", "2017-02-28", "2017-03-31", "2017-04-30", "2017-05-31", "2017-06-30",
                             "2017-07-31", "2017-08-31", "2017-09-30", "2017-10-31", "2017-11-30", "2017-12-31"))


// 來源目錄名稱
val outputPath = "/home/mywh/data/resultData"

// 取出特定日期區間的資料
for (idx <- 0 to 11) {
  //
  val idxDtFrom = dtFrom(0)(idx)
  val idxDtTo = dtTo(0)(idx)
  //
  val sDf = (tDf.where(($"TDate" >= idxDtFrom) && ($"TDate" <= idxDtTo)).
             select("StdNo", "TDate", "Qty", "CarNo", "CusAUnt"))
  //
  println(idx, idxDtFrom, idxDtTo, sDf.count())

  // 針對 客戶編號、車號 及 加油站代號 進行分群，計算加油總量
  val rDf = (sDf.rollup("CusAUnt", "CarNo", "StdNo").
             agg(count("StdNo") as "aStdNoTimes", sum("Qty") as "aQty").
             select("CusAUnt", "CarNo", "StdNo", "aStdNoTimes", "aQty"))
  //
  // rDf.orderBy("CusAUnt", "CarNo", "StdNo").collect().foreach(println)

  // 來源檔案名稱
  val outputFileName = ("CusAUnt" + "CarNo" + "StdNo"
                        + "-" + "aStdNoTimes" + "_" + "aQty" +
                        + "_" + idxDtFrom.replace("-", "") + "-" + idxDtTo.replace("-", ""))
  // 整體目錄及檔案名稱
  val outputFull = outputPath + "/" + outputFileName
  //
  (rDf.orderBy("CusAUnt", "CarNo", "StdNo").
       coalesce(1).
       write.
       format("com.databricks.spark.csv").
       option("header", "true").
       save(outputFull))

  // [null, null, null, 次數, 加油總量]：本月所有客戶的加油次數及加油總量
  // [客戶編號, null, null, 次數, 加油總量]：某客戶的加油次數及加油總量
  // [客戶編號, 車號, null,  次數, 加油總量]：某客戶轄下某車的加油次數及加油總量
  // [客戶編號, 車號, 加油站代號, 次數, 加油總量]:某客戶轄下某車在某個加油站的加油次數及加油總量

}
