//
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//
val inputPath = "/home/mywh/data"
val inputFileName = "tran_master106.csv"
val inputFull = inputPath + "/" + inputFileName
val df = sqlContext.read.format("csv").option("header", "true").load(inputFull)


val dtYear : List[String] = List("2017")
val dtMonth : List[String] = List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
val dtDay : List[String] = List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28")

val dtYear : List[String] = List("2017")
val dtMonth : List[String] = List("01", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
val dtDay : List[String] = List("29", "30", "31")

for (idxYear <- 0 to (dtYear.length - 1)) {
  for (idxMonth <- 0 to (dtMonth.length - 1)) {
    for (idxDay <- 0 to (dtDay.length - 1)) {
      //
      print(dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay))
      print("\n")
      //
      val dtRange = dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay)
      val outputPath = "/home/mywh/data"
      val outputFileName = "tranMaster_" + dtYear(idxYear) + dtMonth(idxMonth) + dtDay(idxDay) + ".csv"
      val outputFll = outputPath + "/" + outputFileName
      //
      val filterDf = df.filter($"Tran_time".contains(dtRange))
      filterDf.write.csv(outputFll)
    }
  }
}

val gDf = tmpDf.groupBy("Deptno", "Tran_time", "Payment").agg(collect_list(struct("Deptno", "Tran_time", "Payment"))).alias("message") 
val tmpDf.collect().foreach(println)
