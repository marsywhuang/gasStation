val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val df = sqlContext.read.format("csv").option("header", "true").load("/Users/apple/data/tran_master.csv")

for (idxYear <- 0 to (dtYear.length - 1)) {
  for (idxMonth <- 0 to (dtMonth.length - 1)) {
    for (idxDay <- 0 to (dtDay.length - 1)) {
      print(dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay))
      print("\n")
      //
      val dtRange = dtYear(idxYear) + "-" + dtMonth(idxMonth) + "-" + dtDay(idxDay)
      val dtFileName = "/Users/apple/data/" + dtYear(idxYear) + dtMonth(idxMonth) + dtDay(idxDay) + ".csv"
      //
      val filterDf = df.filter($"Tran_time".contains(dtRange))
      filterDf.write.csv(dtFileName)
    }
  }
}

val gDf = tmpDf.groupBy("Deptno", "Tran_time", "Payment").agg(collect_list(struct("Deptno", "Tran_time", "Payment"))).alias("message") 
val tmpDf.collect().foreach(println)
