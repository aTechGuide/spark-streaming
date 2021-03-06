# Spark Streaming Kafka

Spark Streaming Kafka Integration

## Commands
- Start Docker Containers -> `docker-compose up`
- Connect to container -> `docker exec -it atechguide-kafka bash`
  - `cd /opt/kafka_2.12-2.4.1/bin/`
- Create a Topic -> `kafka-topics.sh --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic atechguide`
- Attach to console producer 
  - Simple Producer `kafka-console-producer.sh --broker-list localhost:9092 --topic atechguide`
  - Producer With Key / Value `kafka-console-producer.sh --broker-list localhost:9092 --property "parse.key=true" --property "key.separator=:" --topic atechguide_first_topic `
- Connect to Kafka Consumer `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic atechguide`


## Set Up

- Create a checkpoint location `checkpoint` 

# References
- This project contains code snippets / data from following sources
  - [rockthejvm.com  spark-streaming](https://rockthejvm.com/p/spark-streaming)
  
  
# Reference Docs
- [Spark Kafka Doc](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)