# Import some necessary modules
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

HASHTAG_LIST = [
    "#RCB",
    "#CSK",
    "#MI",
    "#IPL",
    "#ELONMUSK"
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
    for hashtag in HASHTAG_LIST:
        # connect kafka consumer to desired kafka topic	
        consumer = KafkaConsumer(hashtag, bootstrap_servers=['localhost:9092'])


        # Parse received data from Kafka
        for msg in consumer:
            record = json.loads(msg.value)
            name = record['name']
            counts = int(record["count"]) 
            timestamp = record["timestamp"]       
            # Create dictionary and ingest data into MongoDB
            try:
                toBeInserted = {
                    "name" : name,
                    "count": counts,
                    "timestamp": timestamp
                }
                id = db.hashtags.insert_one(toBeInserted)
                print("Data inserted with record ids", id)
            except:
                print("Could not insert into MongoDB")


client = MongoClient('localhost',27017)
db = client.temp_data
print("Connected successfully!")
toBeInserted = {
    "name" : "ELONMUSK",
    "count": 69,
    "timestamp": "2019-01-01"
}
id = db.hashtags.insert_one(toBeInserted)
print("Data inserted with record ids", id)
