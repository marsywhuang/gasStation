// 載入函式庫
import org.apache.spark.sql
import org.apache.spark.sql.types._

// 起動
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//
val customSchema = StructType(Array(
  StructField("Deptno", StringType, true),
  StructField("Island", StringType, true), StructField("Gun_No", StringType, true),
  StructField("Tran_time", StringType, true),
  StructField("Tax_Type", StringType, true),
  StructField("Tran_Amt", StringType, true), StructField("Real_Amt", StringType, true), StructField("Inv_Amt", StringType, true),
  StructField("Tax", StringType, true),
  StructField("Discount", StringType, true),
  StructField("Person_Id", StringType, true),
  StructField("Uid", StringType, true),
  StructField("Invoice_Tp", StringType, true),
  StructField("Invoice", StringType, true), StructField("Invoice_Cnt", StringType, true),
  StructField("Ticket_Amt", StringType, true),
  StructField("Payment", StringType, true),
  StructField("Tran_Tp", StringType, true),
  StructField("Car_No", StringType, true),
  StructField("VIP_No", StringType, true),
  StructField("Shift", StringType, true),
  StructField("Ref_No1", StringType, true), StructField("Ref_No2", StringType, true), StructField("Ref_No3", StringType, true), StructField("Ref_No4", StringType, true),
  StructField("Ser_No", StringType, true),
  StructField("VOID", StringType, true),
  StructField("Prt_Invoice", StringType, true),
  StructField("An_Mark", StringType, true),
  StructField("An_Ref1", StringType, true), StructField("An_Ref2", StringType, true),
  StructField("Tran_Mode", StringType, true),
  StructField("Ref_No5", StringType, true),
  StructField("Vip_Apoint", StringType, true), StructField("Vip_Cpoint", StringType, true),
  StructField("Ref_No6", StringType, true), StructField("Ref_No7", StringType, true), StructField("Ref_No8", StringType, true),
  StructField("Vol_Num", StringType, true),
  StructField("Amt_Num", StringType, true),
  StructField("Print_Mark", StringType, true),
  StructField("Carrier_Id1", StringType, true), StructField("Carrier_Id2", StringType, true),
  StructField("Random_Num", StringType, true),
  StructField("Cancel_Reason", StringType, true),
  StructField("Ref_No9", StringType, true), StructField("Ref_No10", StringType, true),
  StructField("Ecr_No", StringType, true),
  StructField("Sha_Card_No", StringType, true),
  StructField("Reverse_fg", StringType, true),
  StructField("Ref_No11", StringType, true), StructField("Ref_No12", StringType, true), StructField("Ref_No13", StringType, true), StructField("Ref_No14", StringType, true), StructField("Ref_No15", StringType, true), StructField("Ref_No16", StringType, true), StructField("Ref_No17", StringType, true)
))

// 資料表名稱
val tableName = "tranMaster"
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
