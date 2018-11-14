// 載入函式庫
import org.apache.spark.sql
import org.apache.spark.sql.types._

// 起動
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//
val customSchema = StructType(Array(
  StructField("Deptno", StringType, true),
  StructField("Island", StringType, true),
  StructField("Gun_No", StringType, true),
  StructField("Tran_Time", StringType, true),
  StructField("Seq", StringType, true),
  StructField("Tax_Type", StringType, true),
  StructField("Product_ID", StringType, true),
  StructField("Class", StringType, true),
  StructField("Price", StringType, true),
  StructField("Amt", StringType, true),
  StructField("Qty", StringType, true),
  StructField("Unit", StringType, true),
  StructField("Ref_No", StringType, true),
  StructField("Shift", StringType, true)
))

//
val tableName = "tranDelt"
// 來源目錄名稱
val inputPath = "/home/cpc/data/resultData"
// 來源目錄名稱
val outputPath = "/home/cpc/data"

//
val dtYear : List[String] = List("2017")
val dtMonth : List[String] = List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
val dtDay : List[String] = List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28")

// 年
for (idxYear <- 0 to (dtYear.length - 1)) {
  // 月
  for (idxMonth <- 0 to (dtMonth.length - 1)) {
    // 日
    for (idxDay <- 0 to (dtDay.length - 1)) {
      //
      print(dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay))
      print("\n")
      
      // 目的目錄名稱
      val inputSubPath = (tableName + "/" +
                          tableName + "_" + dtYear(idxYear) + "/" +
                          tableName + "_" + dtYear(idxYear) + dtMonth(idxMonth))
      val inputFileName = tableName + "_" + dtYear(idxYear) + dtMonth(idxMonth) + dtDay(idxDay) + ".csv"
      // 整體目錄及檔案名稱
      val inputFll = inputPath + "/" + inputSubPath + "/" + inputFileName

      // 整體目錄及檔案名稱
      val inputFull = inputPath + "/" + inputSubPath + "/" + inputFileName
      //
      print(inputFull)
      print("\n")
      // 讀入檔案
      val df = sqlContext.read.format("csv").schema(customSchema).option("header", "false").load(inputFull)
      
      // 來源檔案名稱
      val dtRange = dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay)
      
      // 目的目錄名稱
      val outputSubPath = (tableName + "_" + dtYear(idxYear) + "/" +
                           tableName + "_" + dtYear(idxYear) + dtMonth(idxMonth))
      val outputFileName = tableName + "_" + dtYear(idxYear) + dtMonth(idxMonth) + dtDay(idxDay) + ".csv"
      // 整體目錄及檔案名稱
      val outputFll = outputPath + "/" + outputSubPath + "/" + outputFileName
      //
      print (outputFll)
      print("\n")
      // 輸出檔案具有標題
      df.write.format("com.databricks.spark.csv").option("header", "true").csv(outputFll)
    }
  }
}
