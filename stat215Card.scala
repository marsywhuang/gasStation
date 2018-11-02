// 載入函式庫
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql

// 起動
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// 來源目錄名稱
val inputPath = "/home/mywh/data/rawData"
// 來源檔案名稱
val inputFileName = "215Card.csv"
// 整體目錄及檔案名稱
val inputFull = inputPath + "/" + inputFileName

// 讀入檔案
val df = sqlContext.read.format("csv").option("header", "true").load(inputFull)

