// 載入函式庫
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql

// 起動
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// 來源目錄名稱
val inputPath = "/home/mywh/data"
// 來源檔案名稱
val inputFileName = "215Card.csv"
// 整體目錄及檔案名稱
val inputFull = inputPath + "/" + inputFileName

// 讀入檔案
val df = sqlContext.read.format("csv").option("header", "true").option("schema", "schema").load(inputFull)

// groupby 欄位列表
val groupbyCols = List(List[String]("CUSAUNT", "CARNO", "STDNO"),
                       List[String]("CUSAUNT", "STDNO", "CARNO"))
// agg 欄位列表
val aggCols = List(List[String]("TDATE", "QTY", "MILE"),
                   List[String]("TDATE", "QTY", "MILE"))
// sort 欄位列表
val sortCols = List(List[String]("CUSAUNT", "CARNO", "STDNO"),
                   List[String]("CUSAUNT", "STDNO", "CARNO"))

// 來源目錄名稱
val outputPath = "/home/mywh/data"

//
for ((idxGroupbyItem, idxSortItem) <- groupbyCols.zip(sortCols)) {
  //
  println(s"$idxGroupbyItem, $idxSortItem")
  // 分群
  val gDf = (df.
             groupBy(idxGroupbyItem.head, idxGroupbyItem.tail: _*).
             agg(collect_list(struct("TDATE", "QTY", "MILE")).alias("Message")).
             sort(idxSortItem.head, idxSortItem.tail: _*)
            )
  // 來源檔案名稱
  val outputFileName = idxGroupbyItem + ".json"
  // 整體目錄及檔案名稱
  val outputFull = outputPath + "/" + outputFileName
  // 以 json 格式存檔
  gDf.write.json(outputFull)
}

// 車隊代碼 CusAunt－車號 CarNo－加油站代碼 StdNo－油銷量 Qty
val gDf = (df.groupBy("CUSAUNT", "CARNO", "STDNO").
           agg(collect_list(struct("TDATE", "QTY", "MILE")).alias("Message")).
           sort("CUSAUNT", "CARNO", "STDNO"))

// 來源目錄名稱
val outputPath = "/home/mywh/data"
// 來源檔案名稱
val outputFileName = "CPC215Card_CusAunt-CarNo-StdNo-Qty.json"
// 整體目錄及檔案名稱
val outputFull = outputPath + "/" + outputFileName
// 以 json 格式存檔
gDf.write.json(outputFull)

// 車隊代碼 CusAunt－加油站代碼 StdNo－車號 CarNo－油銷量 Qty

// 車隊代碼 CusAunt－車號 CarNo－油銷量 Qty

// 車隊代碼 CusAunt－加油站代碼 StdNo－油銷量 Qty
// 車號 CarNo－加油站代碼 StdNo－油銷量 Qty

// 加油站代碼 StdNo－車隊代碼 CusAunt－車號 CarNO－油銷量 Qty
// 加油站代碼 StdNo－車隊代碼 CusAunt－油銷量 Qty
// 加油站代碼 StdNo－車號 CarNO－油銷量 Qty

// 車隊代碼－油銷量 Qty
// 車號 CarNO－油銷量 Qty
// 加油站代碼 StdNo－油銷量 Qty
