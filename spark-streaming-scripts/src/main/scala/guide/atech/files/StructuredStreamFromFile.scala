package guide.atech.files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import guide.atech.schema.common._

/**
  * This script create streams from Socket
  *
  * Sending data to socket -> nc -kl 12345
  */
object StructuredStreamFromFile {

  private val spark = SparkSession.builder()
    .appName("")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private def readFromFiles = {
    spark
      .readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

  }

  def main(args: Array[String]): Unit = {

    val lines = readFromFiles

    lines
      .writeStream
      .format("console") // <- Action
      .outputMode("append")
      .start()
      .awaitTermination()
  }




}
