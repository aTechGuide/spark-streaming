name := "SparkStreaming"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.1" % "provided"
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"
libraryDependencies += "org.projectlombok" % "lombok" % "1.16.16"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.2"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.2"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.2"

//lazy val excludeJars = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3" exclude("net.jpountz.lz4", "lz4")

// Configuration Library
libraryDependencies += "com.typesafe" % "config" % "1.4.0"

// Cassandra
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.4.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
