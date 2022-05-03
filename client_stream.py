from base64 import encode
import json
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
# import pyspark.sql.Row as Row
from pyspark.sql import Row
from json import dumps


from kafka import KafkaProducer

sc = SparkContext(appName="spark2kafka")
spark = SparkSession(sc)


producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: 
                         dumps(x).encode('utf-8'), key_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


def countAndPush(rdd):
    spark=SparkSession \
            .builder \
            .config(conf=rdd.context.getConf()) \
            .getOrCreate()

    rowRdd = rdd.map(lambda x: Row(word=x))
    
    if not rowRdd.isEmpty():
        wordsDF = spark.createDataFrame(rowRdd)

        wordsDF.createOrReplaceTempView("words")
        AggregatedWordCountsDF = spark.sql("select word, count(*) as total from words group by word order by 2 desc")       
        # pandasdf=AggregatedWordCountsDF.toPandas()
        AggregatedWordCountsDF.show()
        queryResults = AggregatedWordCountsDF.select("word","total").rdd.flatMap(lambda x: x).collect()
        print(queryResults)
        #publish to kafka pipeline with # as topic
        i=0
        while i <len(queryResults):
            #get in pairs
            #structure of queryResukts will be [word1, total1, word2, total2, word3, total3, ...]
            hashtag = queryResults[i]
            count = queryResults[i+1]
            i+=2
        # for word, count in l:
        #     producer.send(word[1:], count)
        #     # print(i)
            if type(hashtag) == bytes:
                hashtag = hashtag.decode('utf-8')
            print("word: ", hashtag)
            print("count: ", count)
            producer.send(hashtag[1:], count) # remove the # (ahem, hashtag)
            producer.flush()


ssc = StreamingContext(sc,10)
socket_stream = ssc.socketTextStream('localhost', 9008)
lines=socket_stream.window(10)
df=lines.flatMap(lambda x:x.split(" ")).filter(lambda x:x.startswith("#")).filter(lambda x: x.upper() in ['#RCB','#CSK','#SRH','#MI','#ELONMUSK'])

df.foreachRDD(countAndPush)
ssc.start()             
ssc.awaitTermination()  