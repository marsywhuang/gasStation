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
val dtMonth = List(List[String]("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"))
val dtStartDay = List(List[String]("01"))
val dtEndDay = List(List[String]("28", "29", "30", "31"))

// 起始日期
val dtFrom = List(List[String]("2017-01-01", "2017-02-01", "2017-03-01", "2017-04-01", "2017-05-01", "2017-06-01",
                               "2017-07-01", "2017-08-01", "2017-09-01", "2017-10-01", "2017-11-01", "2017-12-01"))
// 終止日期
val dtTo = List(List[String]("2017-01-31", "2017-02-28", "2017-03-31", "2017-04-30", "2017-05-31", "2017-06-30",
                             "2017-07-31", "2017-08-31", "2017-09-30", "2017-10-31", "2017-11-30", "2017-12-31"))


// 來源目錄名稱
// val outputPath = "/home/mywh/data/resultData/CusAUnt-CarNo-StdNo_aStdNoTimes-aQty_2017"
// val outputPath = "/home/mywh/data/resultData/CusAUnt-StdNo-CarNo_aStdNoTimes-aQty_2017"
val outputPath = "/home/mywh/data/resultData/StdNo-CusAUnt-CarNo_aCarNoTimes-aQty_2017"

// groupby 欄位列表
val groupbyCols = List(List[String]("CusAUnt", "CarNo", "StdNo"),
                       List[String]("CusAUnt", "StdNo", "CarNo"),
                       List[String]("StdNo", "CusAUnt", "CarNo"))
// agg 欄位列表
val aggCols = List(List[String]("TDATE", "QTY", "MILE"),
                   List[String]("TDATE", "QTY", "MILE"))

// 取出特定日期區間的資料
for (idx <- 0 to (dtFrom(0).length - 1)) {
  //
  val idxDtFrom = dtFrom(0)(idx)
  val idxDtTo = dtTo(0)(idx)
  //
  val sDf = (tDf.where(($"TDate" >= idxDtFrom) && ($"TDate" <= idxDtTo)).
             select("StdNo", "TDate", "Qty", "CarNo", "CusAUnt"))
  //
  println(idx, idxDtFrom, idxDtTo, sDf.count())

  // 針對 客戶編號、車號 及 加油站代號 進行分群，計算 加油次數 及 加油總量
  // val rDf = (sDf.rollup("CusAUnt", "CarNo", "StdNo").
  //           agg(count("StdNo") as "aStdNoTimes", sum("Qty") as "aQty").
  //           select("CusAUnt", "CarNo", "StdNo", "aStdNoTimes", "aQty"))

  // 針對 客戶編號、加油站代號 及 車號 進行分群，計算 加油次數 及 加油總量
  // val rDf = (sDf.rollup("CusAUnt", "StdNo", "CarNo").
  //            agg(count("StdNo") as "aStdNoTimes", sum("Qty") as "aQty").
  //            select("CusAUnt", "StdNo", "CarNo", "aStdNoTimes", "aQty"))

  // 針對 加油站代號、客戶編號 及 車號 進行分群，計算加油總量
  val rDf = (sDf.rollup("StdNo", "CusAUnt", "CarNo").
             agg(count("CarNo") as "aCarNoTimes", sum("Qty") as "aQty").
             select("StdNo", "CusAUnt", "CarNo", "aCarNoTimes", "aQty"))

  // 來源檔案名稱
  
  // val outputFileName = ("CusAUnt" + "-" + "CarNo" + "-" + "StdNo"
  //                      + "_" + "aStdNoTimes" + "-" + "aQty"
  //                      + "_" + idxDtFrom.replace("-", "") + "-" + idxDtTo.replace("-", ""))

  // val outputFileName = ("CusAUnt" + "-" + "StdNo" + "-" + "CarNo"
  //                       + "_" + "aStdNoTimes" + "-" + "aQty"
  //                       + "_" + idxDtFrom.replace("-", "") + "-" + idxDtTo.replace("-", ""))

  val outputFileName = ("StdNo" + "-" + "CusAUnt" + "-" + "CarNo"
                        + "_" + "aStdNoTimes" + "-" + "aQty"
                        + "_" + idxDtFrom.replace("-", "") + "-" + idxDtTo.replace("-", ""))

  // 整體目錄及檔案名稱
  val outputFull = outputPath + "/" + outputFileName

  //
  // (rDf.orderBy("CusAUnt", "CarNo", "StdNo").
  //      coalesce(1).
  //      write.
  //      format("com.databricks.spark.csv").
  //      option("header", "true").
  //      save(outputFull))

  //
  // (rDf.orderBy("CusAUnt", "StdNo", "CarNo").
  //      coalesce(1).
  //      write.
  //      format("com.databricks.spark.csv").
  //      option("header", "true").
  //      save(outputFull))

  //
  (rDf.orderBy("StdNo", "CusAUnt", "CarNo").
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


// 取出 加油站代號、交易日期、量、車號、企業客戶代號
val pDf = df.select("StdNo", "TDate", "Qty", "CarNo", "CusAUnt")
// 自 TDate 分離出 年 及 月 到欄位 TDateYear 及 TDateMonth
val tDf = pDf.select(col("*"), substring(col("TDate"), 0, 4).as("TDateYear"), substring(col("TDate"), 5, 2).as("TDateMonth"))
// TDate 欄位屬性成為 date 型別，以及更改 Qty 欄位屬性成為 float 型別
val sDf = (tDf.withColumn("TDate", to_date($"TDate", "yyyyMMdd")).
           withColumn("Qty", df("Qty").cast(sql.types.FloatType)))

// 目的目錄
val outputPath = "/home/mywh/data/resultData"
//
val dtYear = List(List[String]("2016", "2017", "2018"))
val groupbyColumn = List(List[String]("CusAUnt", "CarNo", "StdNo"),
                         List[String]("CusAUnt", "StdNo", "CarNo"),
                         List[String]("StdNo", "CusAUnt", "CarNo"))
dtYear(0).zip(groupbyColumn)
//
for (idxYear <- 0 to (dtYear(0).length - 1)) {
  //
  val tmpIdxYear = dtYear(0)(idxYear)
//  // 針對 客戶編號、車號 及 加油站代號 進行分群，計算 加油次數 及 加油總量
//  val rDf = (sDf.rollup("TDateYear", "TDateMonth", "CusAUnt", "CarNo", "StdNo").
//             agg(count("StdNo") as "aStdNoTimes", sum("Qty") as "aQty").
//             select("*").filter(s"TDateYear = $tmpIdxYear"))
//  // 檔案名稱
//  val outputFileName = ("TDateYear" + "-" + "TDateMonth" + "-" + "CusAUnt" + "-" + "CarNo" + "-" + "StdNo"
//                        + "_" + "aStdNoTimes" + "-" + "aQty"
//                        + "_" + tmpIdxYear)
//  // 整體目錄及檔案名稱
//  val outputFull = outputPath + "/" + outputFileName
//  //
//  (rDf.orderBy("TDateYear", "TDateMonth", "CusAUnt", "CarNo", "StdNo").
//       coalesce(1).
//       write.
//       format("com.databricks.spark.csv").
//       option("header", "true").
//       save(outputFull))

//  // 針對 客戶編號、加油站代號 及 車號 進行分群，計算 加油次數 及 加油總量
//  val rDf = (sDf.rollup("TDateYear", "TDateMonth", "CusAUnt", "StdNo", "CarNo").
//             agg(count("StdNo") as "aStdNoTimes", sum("Qty") as "aQty").
//             select("*").filter(s"TDateYear = $tmpIdxYear"))
//  // 檔案名稱
//  val outputFileName = ("TDateYear" + "-" + "TDateMonth" + "-" + "CusAUnt" + "-" + "StdNo" + "-" + "CarNo"
//                        + "_" + "aStdNoTimes" + "-" + "aQty"
//                        + "_" + tmpIdxYear)
//  // 整體目錄及檔案名稱
//  val outputFull = outputPath + "/" + outputFileName
//  //
//  (rDf.orderBy("TDateYear", "TDateMonth", "CusAUnt", "StdNo", "CarNo").
//       coalesce(1).
//       write.
//       format("com.databricks.spark.csv").
//       option("header", "true").
//       save(outputFull))

  // 針對 客戶編號、加油站代號 及 車號 進行分群，計算 加油次數 及 加油總量
  val rDf = (sDf.rollup("TDateYear", "TDateMonth", "StdNo", "CusAUnt", "CarNo").
             agg(count("StdNo") as "aStdNoTimes", sum("Qty") as "aQty").
             select("*").filter(s"TDateYear = $tmpIdxYear"))
  // 檔案名稱
  val outputFileName = ("TDateYear" + "-" + "TDateMonth" + "-" + "StdNo" + "-" + "CusAUnt" + "-" + "CarNo"
                        + "_" + "aStdNoTimes" + "-" + "aQty"
                        + "_" + tmpIdxYear)
  // 整體目錄及檔案名稱
  val outputFull = outputPath + "/" + outputFileName
  //
  (rDf.orderBy("TDateYear", "TDateMonth", "StdNo", "CusAUnt", "CarNo").
       coalesce(1).
       write.
       format("com.databricks.spark.csv").
       option("header", "true").
       save(outputFull))

}

// 車隊卡當月油銷級距分析表：年度月油品（汽油、柴油）銷售總量

// 取出 加油站代號、交易日期、量、車號、企業客戶代號、產品
val pDf = df.select("StdNo", "TDate", "Qty", "CarNo", "CusAUnt", "PNo")
// 自 TDate 分離出 年 及 月 到欄位 TDateYear 及 TDateMonth
val tDf = pDf.select(col("*"), substring(col("TDate"), 0, 4).as("TDateYear"), substring(col("TDate"), 5, 2).as("TDateMonth"))
// TDate 欄位屬性成為 date 型別，以及更改 Qty 欄位屬性成為 float 型別
val sDf = (tDf.withColumn("TDate", to_date($"TDate", "yyyyMMdd")).
           withColumn("Qty", df("Qty").cast(sql.types.FloatType)))

//
val dtYear = List(List[String]("2016", "2017", "2018"))
//
for (idxYear <- 0 to (dtYear(0).length - 1)) {
  //
  val tmpIdxYear = dtYear(0)(idxYear)
  //
  val rDf = (sDf.rollup("TDateYear", "TDateMonth", "PNo").
             agg(sum("Qty") as "aQty").select("*").filter(s"TDateYear = $tmpIdxYear").
             orderBy("TDateYear", "TDateMonth", "PNo"))
  //
  rDf.collect().foreach(println)
}

// 加油站資訊

// 來源目錄名稱
val inputPath = "/home/mywh/data/rawData"
// 來源檔案名稱
val inputFileName = "215Card.csv"
// 整體目錄及檔案名稱
val inputFull = inputPath + "/" + inputFileName

// 讀入檔案
val df215Card = sqlContext.read.format("csv").option("header", "true").load(inputFull)
// 取出 加油站代號、交易日期、量、車號、企業客戶代號
val pDf215Card = df215Card.select("StdNo", "TDate", "Qty", "CarNo", "CusAUnt")

// TDate 欄位屬性成為 date 型別，以及更改 Qty 欄位屬性成為 float 型別
val tDf215Card = (pDf215Card.withColumn("TDate", to_date($"TDate", "yyyyMMdd")).
                  withColumn("Qty", df("Qty").cast(sql.types.FloatType)))

// 來源檔案名稱
val inputFileName = "infoCpcGasStation.csv"
// 整體目錄及檔案名稱
val inputFull = inputPath + "/" + inputFileName

// 讀入檔案
val dfInfoCpcGasStation = sqlContext.read.format("csv").option("header", "false").load(inputFull)
// 取出欄位名稱
val tmpHeader = dfInfoCpcGasStation.first()
// 刪除欄位名稱
val tmpDfInfoCpcGasStation = dfInfoCpcGasStation.filter(row => row != tmpHeader)
val dfInfoCpcGasStation = tmpDfInfoCpcGasStation

//
val pDf215Card = df.select("StdNo", "TDate", "Qty", "CarNo", "CusAUnt", "PNo")
val pDfInfoGasStation = dfInfoCpcGasStation.select("_c0", "_c1", "_c3", "_c4") // _c0 站代號、_c1 類別、_c3 縣市、_c4 鄉鎮區
val joinedDF = pDf215Card.as('a).join(pDfInfoGasStation.as('b), $"a.StdNo" === $"b._c0").drop("_c0")
joinedDF.show()
