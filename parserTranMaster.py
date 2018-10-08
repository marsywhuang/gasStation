#
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

#
dfTranMaster = sqlContext.read.csv("/home/mywh/data/tranMaster_201801.csv", header=False)

#
listStd = dfTranMaster.select(["_c0"]).distinct()

#
for idxItem in listStd.collect():
