name := "spark-streaming-postgres"

version := "0.1"

scalaVersion := "2.12.10"

// Library Versions
val sparkVersion = "3.0.0-preview"
val postgresVersion = "42.2.2"

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

  // postgres
  "org.postgresql" % "postgresql" % postgresVersion,
)

// Configuration Library
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
