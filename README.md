# DBT-Project-UE19CS344

#### Team Members:
- [Anurag Khanra](https://github.com/anuragisfree)
- [Arvind Krishna](https://github.com/Arvind)
- [Aranya Bhalla](https://github.com/aranyabhalla)
- [Rahul Makhija](https://github.com/rahulmakhija30)

#### Instructions to run
Install the following
- Spark
- Kafka
- MongoDB


Run the following commands in different terminals or push them to background
```bash
# navigate to kafka location firstly
sudo bin/zookeeper-server-start.sh config/zookeeper.properties # start zookeeper

sudo bin/kafka-server-start.sh config/server.properties # start kafka

bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092 # describe topic


python3 stream.py #start python client to get twitter data

python3 client_stream.py # start python client for spark client and kafka producer

python3 mongo_client.py # start python client for inserting to database
```

#### Port Mappings:

- 9008 -> Stream from twitter API to spark app
- 9092 -> Kafka endpoints
- 27017 -> Mongo client