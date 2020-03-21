name := "spark-streaming-cassandra"

version := "0.1"

scalaVersion := "2.12.10"

// Library Versions
val sparkVersion = "3.0.0-preview"
val cassandraConnectorVersion = "2.4.2"

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
    // cassandra - this version officially works with Spark 2.4, but tested with Spark 3.0-preview as well
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,

  
)

// Configuration Library
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
