import json
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
# import pyspark.sql.Row as Row
from pyspark.sql import Row


from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from collections import Counter

producer = KafkaProducer(bootstrap_servers='localhost:9009', value_serializer=str.encode, key_serializer=str.encode)



conf = SparkConf()
#set name for our app
conf.setAppName("ConnectingDotsSparkKafkaStreaming")
#The master URL to connect
conf.setMaster('localhost:7077')
sc = None
try:
        sc.stop()
        sc = SparkContext(conf=conf)
except:
        sc = SparkContext(conf=conf)

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
        l = wordCountsDataFrame.select("word","total").rdd.flatMap(lambda x: x).collect()
        print(l)

        #push this to kafka
        for word, count in l:
                producer.send(word, count)
                # print(i)

        #list_of_processed_events = wordsDataFrame.collect()
        # producer.send('output_event', value = str(list_of_processed_events))
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

#processing each micro batch
def process_events(event):
    return (event[0], Counter(event[1].split(" ")).most_common(3))


ssc = StreamingContext(sc,10)
socket_stream = ssc.socketTextStream('localhost', 9008)
lines=socket_stream.window(10)
df=lines.flatMap(lambda x:x.split(" ")).filter(lambda x:x.startswith("#")).filter(lambda x: x in ['#RCB','#CSK','#MI','#SRH'])
# countdf = df.countByValue()
# countdf.pprint()

kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'test-consumer-group', {'input_event':1})
lines = kafkaStream.map(lambda x : process_events(x))


df.foreachRDD(process)
ssc.start()             
ssc.awaitTermination()  
