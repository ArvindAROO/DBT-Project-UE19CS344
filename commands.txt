
sudo bin/zookeeper-server-start.sh config/zookeeper.properties // start zookeeper

sudo bin/kafka-server-start.sh config/server.properties // start kafka

sudo bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092 --zookeeper localhost:218 // start consumer on shell

sudo bin/kafka-console-producer.sh --topic quickstart-events --broker-list localhost:9092 // start producer on shell

sudo ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test // create topic manually

cd /usr/local/kafka //change directory

bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092 // describe topic

python3 client.py

python3 client_stream.py

python3 mongo_client.py
