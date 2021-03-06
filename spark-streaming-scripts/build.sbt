name := "spark-streaming-scripts"

version := "0.1"

scalaVersion := "2.12.10"

// Library Versions
val sparkVersion = "3.0.0-preview"
val cassandraConnectorVersion = "2.4.2"
val twitter4jVersion = "4.0.4"
val twitterBahirVersion = "2.4.0"
val configVersion = "1.4.0"

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
  
  // mllib
  "org.apache.spark" %% "spark-mllib" % sparkVersion,

  // Cassandra Driver
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,

  // twitter
  "org.twitter4j" % "twitter4j-core" % twitter4jVersion,
  "org.twitter4j" % "twitter4j-stream" % twitter4jVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % twitterBahirVersion,

  // Config Library
  "com.typesafe" % "config" % configVersion

)
