import json
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
import pandas as pd
from pyspark.sql import Row

sc = SparkContext(appName="PySparkShell")
spark = SparkSession(sc)

ssc = StreamingContext(sc,1)
lines = ssc.socketTextStream('localhost', 9008)
lines.foreachRDD(lambda rdd: rdd.foreach(lambda x: print(x)))
ssc.start()             
ssc.awaitTermination()  
