from base64 import encode
import json
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
# import pyspark.sql.Row as Row
from pyspark.sql import Row
from json import dumps


from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from collections import Counter

sc = SparkContext(appName="PySparkShell")
spark = SparkSession(sc)

#setup a kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: 
                         dumps(x).encode('utf-8'), key_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


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
        l = wordCountsDataFrame.select("word","total").rdd.flatMap(lambda x: x).collect()
        print(l)
        #publish to kafka pipeline with word as topic
        i=0
        while i <len(l):
            word = l[i]
            count = l[i+1]
            i+=2
        # for word, count in l:
        #     producer.send(word[1:], count)
        #     # print(i)
            if type(word) == bytes:
                word = word.decode('utf-8')
            print("word: ", word)
            print("count: ", count)
            producer.send(word[1:], count)
            producer.flush()


ssc = StreamingContext(sc,10)
socket_stream = ssc.socketTextStream('localhost', 9008)
lines=socket_stream.window(10)
df=lines.flatMap(lambda x:x.split(" ")).filter(lambda x:x.startswith("#")).filter(lambda x: x.upper() in ['#RCB','#CSK','#SRH','#MI','#ELONMUSK'])

df.foreachRDD(process)
ssc.start()             
ssc.awaitTermination()  