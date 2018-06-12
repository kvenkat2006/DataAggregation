# DataAggregation


## Start the Zookeeper
#### Instead of this: bin/zookeeper-server-start.sh config/zookeeper.properties, run the following:
nohup ~/kafka_2.11-0.10.2.1/bin/zookeeper-server-start.sh ~/kafka_2.11-0.10.2.1/config/zookeeper.properties > ~/kafka_2.11-0.10.2.1/zookeeperCaravan.log 2>&1 &



## Start the Kafka broker
#### Instead of this:  bin/kafka-server-start.sh config/server.properties, run the following:
nohup ~/kafka_2.11-0.10.2.1/bin/kafka-server-start.sh ~/kafka_2.11-0.10.2.1/config/server.properties > ~/kafka_2.11-0.10.2.1/kafkaCaravan.log 2>&1 &



## To stop the broker and zookeeper:
~/kafka_2.11-0.10.2.1/bin/kafka-server-stop.sh ~/kafka_2.11-0.10.2.1/config/server.properties
~/kafka_2.11-0.10.2.1/bin/zookeeper-server-stop.sh ~/kafka_2.11-0.10.2.1/config/zookeeper.properties


##On Google Compute Engine: Copy your project to your home directory on the GCE Virtual Machine:
cd /home/vennkumar/KafkaStructExample

java -Dlog4j.configuration="file:~/KafkaStructExample/src/main/resources/log4j.xml" -cp target/uber-kafkastructstream-1.0-SNAPSHOT.jar com.dhee.DataAggrProto


## Run Consumer:
cd /home/kumar/DataAggregation

# DataAggrProto
java -Dlog4j.configuration="file:src/main/resources/log4j.xml" -cp target/uber-kafkastructstream-1.0-SNAPSHOT.jar com.dhee.DataAggrProto

#Static Data Aggregator:
java -Dlog4j.configuration="file:src/main/resources/log4j.xml" -cp target/uber-kafkastructstream-1.0-SNAPSHOT.jar com.dhee.StaticDataAggrProto


## Run Producer:
# BASE:
java -Dlog4j.configuration="file:src/main/resources/log4j.xml" -cp target/uber-kafkastructstream-1.0-SNAPSHOT.jar com.dhee.Producer BASE

java -Dlog4j.configuration="file:src/main/resources/log4j.xml" -cp target/uber-kafkastructstream-1.0-SNAPSHOT.jar com.dhee.Producer WHATIF

