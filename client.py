import json
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
sc = SparkContext(appName="PySparkShell")
spark = SparkSession(sc)

# def process(rdd):
#         spark=SparkSession \
#                 .builder \
#                 .config(conf=rdd.context.getConf()) \
#                 .getOrCreate()
    
#         rowRdd = rdd.map(lambda x: Row(word=x))
#         if not rowRdd.isEmpty():
#             wordsDataFrame = spark.createDataFrame(rowRdd)
    
#             wordsDataFrame.createOrReplaceTempView("words")
#             wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word order by 2 desc")       
#             pd_df=wordCountsDataFrame.toPandas()
#             wordCountsDataFrame.show()

ssc = StreamingContext(sc,100)
socket_stream = ssc.socketTextStream('localhost', 9008)
lines=socket_stream.window(100)
df=lines.flatMap(lambda x:x.split(" ")).filter(lambda x:x.startswith("#")).filter(lambda x: x in ['#RCB','#CSK','#MI','#SRH'])
countdf = df.countByValue()
countdf.pprint()

ssc.start()             
ssc.awaitTermination()  
