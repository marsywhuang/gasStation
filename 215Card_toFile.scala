// 載入函式庫
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql

// 起動
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// 來源目錄名稱
val inputPath = "/home/cpc/data/rawData"
// 來源檔案名稱
val inputFileName = "215Card.csv"
// 整體目錄及檔案名稱
val inputFull = inputPath + "/" + inputFileName

// 讀入檔案
val df = sqlContext.read.format("csv").option("header", "true").load(inputFull)

//
val dtYear : List[String] = List("2017")
val dtMonth : List[String] = List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
val dtDay : List[String] = List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28")

//
val outputPath = "/home/cpc/data/resultData"
//
for (idxYear <- 0 to (dtYear.length - 1)) {
  for (idxMonth <- 0 to (dtMonth.length - 1)) {
    for (idxDay <- 0 to (dtDay.length - 1)) {
      //
      print(dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay))
      print("\n")
      //
      val dtRange = dtYear(idxYear) + dtMonth(idxMonth) + dtDay(idxDay)
      val outputFileName = "215Card_" + dtYear(idxYear) + dtMonth(idxMonth) + dtDay(idxDay) + ".csv"
      val outputFll = outputPath + "/" + outputFileName
      //
      val filterDf = df.filter($"TDate".contains(dtRange))
      // 輸出檔案具有標題
      filterDf.write.format("com.databricks.spark.csv").option("header", "true").csv(outputFll)
    }
  }
}
