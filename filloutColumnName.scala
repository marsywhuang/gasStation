//
import org.apache.spark.sql.types._
//
val customSchema = StructType(Array(
  StructField("ID", StringType, true),
  StructField("CTYPE", StringType, true),
  StructField("STDNO", StringType, true),
  StructField("PNO", StringType, true),
  StructField("UNT", StringType, true),
  StructField("LDATE", StringType, true), StructField("TDATE", StringType, true),
  StructField("QTY", StringType, true),
  StructField("STRNO", StringType, true),
  StructField("TNO", StringType, true),
  StructField("MRK3", StringType, true), StructField("MRK4", StringType, true), StructField("MRK5", StringType, true),
  StructField("TICKETNO", StringType, true),
  StructField("SHIP", StringType, true),
  StructField("CARNO", StringType, true),
  StructField("PNAME", StringType, true),
  StructField("TICKETTYPE", StringType, true),
  StructField("CUSAUNT", StringType, true),
  StructField("CUSMUNT", StringType, true),
  StructField("TTIME", StringType, true),
  StructField("CARDMNO", StringType, true),
  StructField("CTYPEMK", StringType, true),
  StructField("MILE", StringType, true),
  StructField("PRICE", StringType, true),
  StructField("CNO", StringType, true),
  StructField("BILLNO", StringType, true),
  StructField("ATYPE", StringType, true), StructField("ADATE", StringType, true), StructField("RDATE", StringType, true), StructField("MDATE", StringType, true),
  StructField("SID", StringType, true),
  StructField("YYMM", StringType, true),
  StructField("BPRICE", StringType, true), StructField("SPRICE", StringType, true),
  StructField("MK1", StringType, true), StructField("MK2", StringType, true), StructField("MK3", StringType, true),
  StructField("S3_SEQNO", StringType, true),
  StructField("ISLAND_NO", StringType, true), StructField("GUN_NO", StringType, true),
  StructField("EDC_VERSION", StringType, true)
))

// 來源目錄名稱
val inputPath = "/home/mywh/data/resultData"
// 來源目錄名稱
val outputPath = "/home/mywh/data"
//
val dtYear : List[String] = List("2017")
val dtMonth : List[String] = List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
val dtMonth : List[String] = List("02")
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
      val inputSubPath = ("215Card" + "/" +
                          "215Card_" + dtYear(idxYear) + "/" +
                          "215Card_" + dtYear(idxYear) + dtMonth(idxMonth))
      val inputFileName = "215Card_" + dtYear(idxYear) + dtMonth(idxMonth) + dtDay(idxDay) + ".csv"
      // 整體目錄及檔案名稱
      val inputFll = inputPath + "/" + inputSubPath + "/" + inputFileName

      // 整體目錄及檔案名稱
      val inputFull = inputPath + "/" + inputSubPath + "/" + inputFileName
      //
      // print(inputFull)
      // print("\n")
      // 讀入檔案
      val df = sqlContext.read.format("csv").schema(customSchema).option("header", "false").load(inputFull)
      
      // 來源檔案名稱
      val dtRange = dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay)
      
      // 目的目錄名稱
      val outputSubPath = ("215Card_" + dtYear(idxYear) + "/" +
                           "215Card_" + dtYear(idxYear) + dtMonth(idxMonth))
      val outputFileName = "215Card_" + dtYear(idxYear) + dtMonth(idxMonth) + dtDay(idxDay) + ".csv"
      // 整體目錄及檔案名稱
      val outputFll = outputPath + "/" + outputSubPath + "/" + outputFileName
      //
      print (outputFll)
      print("\n")
      // 來源檔案名稱
      // val filterDf = df.filter($"Tran_Time".contains(dtRange))
      // 輸出檔案具有標題
      df.write.format("com.databricks.spark.csv").option("header", "true").csv(outputFll)
    }
  }
}



