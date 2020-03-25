package guide.atech.eventTime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Processing Time
  * - Time when record arrives at spark
  *
  * Remember
  *  - Window duration and sliding interval must be a multiple of the batch interval
  */
object ProcessingTimeWindows {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private def readFromSocket = {

    spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

  }

  // Counting Characters every 10s by processing time
  def aggregateByProcessingTime(): Unit = {

    val linesCharCountByWindowDF = readFromSocket
      .select(col("value"), current_timestamp().as("processingTime"))
      .groupBy(window(col("processingTime"), "10 seconds").as("window"))
      .agg(sum(length(col("value"))).as("charCount"))
      .selectExpr("window.*", "charCount")


    linesCharCountByWindowDF
      .writeStream
      .option("numRows", 50)
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    aggregateByProcessingTime()
  }

}
