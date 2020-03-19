name := "SparkStreaming"

version := "0.1"

scalaVersion := "2.12.10"

// Library Versions
val sparkVersion = "3.0.0-preview"
val cassandraConnectorVersion = "2.4.2"
val twitter4jVersion = "4.0.4"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  // streaming-kafka
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

  // cassandra - this version officially works with Spark 2.4, but tested with Spark 3.0-preview as well
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,

  // twitter
  "org.twitter4j" % "twitter4j-core" % twitter4jVersion,
  "org.twitter4j" % "twitter4j-stream" % twitter4jVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
)

// Configuration Library
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
// libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
