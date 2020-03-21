# Spark Streaming Cassandra

Spark Streaming Cassandra Integration

## Commands
- Start Docker Containers -> `docker-compose up`
- Connect to container -> `./cql.sh`
- Create keyspace -> `create KEYSPACE public with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};`
- Create Table -> `CREATE TABLE public.cars("Name" text PRIMARY KEY , "Horsepower" int);`
- Delete rows -> `TRUNCATE public.cars;`

## Set Up
- Create a checkpoint location `checkpoint`

# References
- This project contains code snippets / data from following sources
  - [rockthejvm.com  spark-streaming](https://rockthejvm.com/p/spark-streaming)
  
  