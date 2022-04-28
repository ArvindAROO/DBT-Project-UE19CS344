# Import some necessary modules
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

HASHTAG_LIST = [
    "RCB",
    "rcb",
    "CB"
]

def main():
    # Connect to MongoDB and pizza_data database
    try:
        client = MongoClient('localhost',27017)
        db = client.temp_data
        print("Connected successfully!")
    except:  
        print("Could not connect to MongoDB")
        return    
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
    consumer.subscribe(["RCB","CSK","MI","SRH","ELONMUSK"])
    # consumer.subscribe("CB")
    # consumer.subscribe("CSK")
    # consumer.subscribe("MI")
    # consumer.subscribe("SRH")
    # consumer.subscribe("ElonMusk")

    print("hello")

        # Parse received data from Kafka
    for msg in consumer:
        print(msg)
        #record = json.loads(msg.value)
        name = msg.topic
        counts = int(msg.value)      
        # Create dictionary and ingest data into MongoDB
        try:
            toBeInserted = {
                "name" : name,
                "count": counts
            }
            id = db.hashtags.insert_one(toBeInserted)
            print("Data inserted with record ids", id)
        except:
            print("Could not insert into MongoDB")
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

main()
