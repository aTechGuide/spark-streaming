package guide.atech.socket

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import scala.concurrent.duration._

/**
  * This script create streams from Socket
  *
  * Sending data to socket -> nc -kl 12345
  */
object Triggers {

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

    lines
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        // Trigger.ProcessingTime(2.seconds) // every 2 s run the query
        // Trigger.Once() // Single batch then terminate
        Trigger.Continuous(2.seconds) // experimental, creates a batch every two seconds regardless of whatever we have
      )
      .start()
      .awaitTermination()

    /**
      * Trigger.ProcessingTime(2.seconds) -> Every two seconds we check for new data
      * - If data found at trigger point: all the transformations are executed in new batch
      * - If no data found, we will simply wait for new trigger
      */

  }




}
