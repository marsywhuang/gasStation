val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val df = sqlContext.read.format("csv").option("header", "true").load("/Users/apple/data/tran_master.csv")

val filterDf = df.filter($"Tran_time".contains("2018-02-01"))
filterDf.write.csv("/Users/apple/data/tranMaster_20180202.csv")

val gDf = tmpDf.groupBy("Deptno", "Tran_time", "Payment").agg(collect_list(struct("Deptno", "Tran_time", "Payment"))).alias("message") 
val tmpDf.collect().foreach(println)
