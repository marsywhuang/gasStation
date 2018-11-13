// 載入函式庫
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql

// 起動
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// 來源目錄名稱
val inputPath = "/home/cpc/data/rawData"
// 來源檔案名稱
val inputFileName = "tran_detl106.csv"
// 整體目錄及檔案名稱
val inputFull = inputPath + "/" + inputFileName
// 讀入檔案
val df = sqlContext.read.format("csv").option("header", "true").load(inputFull)

val dtYear : List[String] = List("2017")
val dtMonth : List[String] = List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
val dtDay : List[String] = List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28")

// 目的目錄名稱
val outputPath = "/home/cpc/data/resultDate"
//
for (idxYear <- 0 to (dtYear.length - 1)) {
  for (idxMonth <- 0 to (dtMonth.length - 1)) {
    for (idxDay <- 0 to (dtDay.length - 1)) {
      //
      print(dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay))
      print("\n")
      // 來源檔案名稱
      val dtRange = dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay)
      // 目的目錄名稱
      val outputFileName = "tranDelt_" + dtYear(idxYear) + dtMonth(idxMonth) + dtDay(idxDay) + ".csv"
      // 整體目錄及檔案名稱
      val outputFll = outputPath + "/" + outputFileName
      // 來源檔案名稱
      val filterDf = df.filter($"Tran_Time".contains(dtRange))
      // 輸出檔案具有標題
      filterDf.write.format("com.databricks.spark.csv").option("header", "true").csv(outputFll)
    }
  }
}
