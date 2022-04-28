import json
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
# import pyspark.sql.Row as Row
from pyspark.sql import Row
sc = SparkContext(appName="PySparkShell")
spark = SparkSession(sc)

def process(rdd):
    spark=SparkSession \
            .builder \
            .config(conf=rdd.context.getConf()) \
            .getOrCreate()

    rowRdd = rdd.map(lambda x: Row(word=x))
    if not rowRdd.isEmpty():
        wordsDataFrame = spark.createDataFrame(rowRdd)

        wordsDataFrame.createOrReplaceTempView("words")
        wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word order by 2 desc")       
        # pd_df=wordCountsDataFrame.toPandas()
        wordCountsDataFrame.show()

        # # df = pd_df.toPandas()
        # df.pprint()
        # try:
        #     my_dict = wordCountsDataFrame.to_dict(orient = 'list')
        #     print(my_dict)
        # except:
        #     print("30 failed")
        # try:
        #     print(wordCountsDataFrame.to_json(orient='records'))
        # except:
        #     print("34 failed")



ssc = StreamingContext(sc,10)
socket_stream = ssc.socketTextStream('localhost', 9008)
lines=socket_stream.window(10)
df=lines.flatMap(lambda x:x.split(" ")).filter(lambda x:x.startswith("#")).filter(lambda x: x in ['#RCB','#CSK','#MI','#SRH'])
# countdf = df.countByValue()
# countdf.pprint()
df.foreachRDD(process)
ssc.start()             
ssc.awaitTermination()  
