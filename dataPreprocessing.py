#
inputPath = "/home/mywh/data/rawData"
inputFile = "infoCpcGasStation.csv"
inputFull = inputPath + "/" + inputFile
#
df = sqlContext.read.csv(inputFull, encoding = 'utf-8', header = "true")
