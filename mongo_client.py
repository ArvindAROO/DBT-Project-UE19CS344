from tkinter.messagebox import NO
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
# HASHTAG_LIST = [
#     "RCB",
#     "rcb",
#     "CB"
# ]

def main():
    try:
        client = MongoClient('localhost',27017)
        db = client.temp_data
        print("Connection established at port 27017")
    except:  
        print("lol, connection failed")
        return    
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
    consumer.subscribe(["RCB","CSK","MI","SRH","ELONMUSK"])
    # consumer.subscribe("CB")
    # consumer.subscribe("CSK")
    # consumer.subscribe("MI")
    # consumer.subscribe("SRH")
    # consumer.subscribe("ElonMusk")

    print("hello")

    for msg in consumer:
        # print(msg)
        name = msg.topic
        counts = int(msg.value)      
        # Create dictionary and ingest data into MongoDB
        timeStamp = int(time.time())
        try:
            toBeInserted = {
                "name" : name,
                "count": counts,
                "time": timeStamp

            }
            id = db.hashtags.insert_one(toBeInserted)
            print("Data: {} inserted with id:".format(toBeInserted, id))
        except:
            print("Mongo insertion failed")
    #consumer.close()


# client = MongoClient('localhost',27017)
# db = client.temp_data
# print("Connected successfully!")
# toBeInserted = {
#     "name" : "ELONMUSK",
#     "count": 69,
#     "timestamp": "2019-01-01"
# }
# id = db.hashtags.insert_one(toBeInserted)
# print("Data inserted with record ids", id)

def batchProcessing(initialTimeStamp, finalTimeStamp):
    # aggregate data from MongoDB between initialTimeStamp and finalTimeStamp
    # return a list of tuples (hashtag, count)
    MongoClient = MongoClient('localhost',27017)
    db = MongoClient.temp_data
    query= None
    if type(initialTimeStamp) == int and type(finalTimeStamp) == int:
        query = {"time": {"$gte": initialTimeStamp, "$lte": finalTimeStamp}}
    else:
        query = {"time": {"$gte": initialTimeStamp, "$lte": finalTimeStamp}}
    queryOutput = db.hashtags.find(query)
    resultDic = {}
    for i in queryOutput:
        if i["name"] in resultDic:
            resultDic[i["name"]] += i["count"]
        else:
            resultDic[i["name"]] = i["count"]
    return resultDic

if __name__ == "__main__":
    main()
    # print(batchProcessing(0, 100000000000000))
