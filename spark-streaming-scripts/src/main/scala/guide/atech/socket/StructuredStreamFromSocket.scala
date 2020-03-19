package guide.atech.socket

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * This script create streams from Socket
  *
  * Sending data to socket -> nc -kl 12345
  */
object StructuredStreamFromSocket {

  private val spark = SparkSession.builder()
    .appName("")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private def readFromScoket = {
    spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

  }

  def main(args: Array[String]): Unit = {

    val lines = readFromScoket

    // transformation
    val shortLines = lines.filter(length(col("value")) <= 5)

    // Tell static vs streaming dataframe
    println(shortLines.isStreaming)

    val query: StreamingQuery = shortLines
      .writeStream
      .format("console") // <- Action
      .outputMode("append")
      .start() //<- Start Command is Async. So we need awaitTermination else program will stop before outputting to console

    // Wait for stream to finish
    query.awaitTermination()
  }




}
