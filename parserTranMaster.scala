val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val df = sqlContext.read.format("csv").option("header", "true").load("/Users/apple/data/tran_master.csv")


val dtYear : List[String] = List("2018")
val dtMonth : List[String] = List("01")
val dtDay : List[String] = List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31")

for (idxYear <- 0 to (dtYear.length - 1)) {
  for (idxMonth <- 0 to (dtMonth.length - 1)) {
    for (idxDay <- 0 to (dtDay.length - 1)) {
      print(dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay))
      print("\n")
      //
      val dtRange = dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay)
      val dtPath = "/Users/apple/data/"
      val dtFileName = "tranMaster_"+ dtYear(idxYear) + dtMonth(idxMonth) + dtDay(idxDay) + ".csv"
      val dtFull = dtPath + "/" + dtFileName
      //
      val filterDf = df.filter($"Tran_time".contains(dtRange))
      filterDf.write.csv(dtFileName)
    }
  }
}

val gDf = tmpDf.groupBy("Deptno", "Tran_time", "Payment").agg(collect_list(struct("Deptno", "Tran_time", "Payment"))).alias("message") 
val tmpDf.collect().foreach(println)
