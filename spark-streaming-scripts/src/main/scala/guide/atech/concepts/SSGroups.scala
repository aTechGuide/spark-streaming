package guide.atech.concepts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SSGroups {

  private val spark = SparkSession.builder()
    .appName("")
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

  def main(args: Array[String]): Unit = {

    val lines = readFromSocket

    val names = lines.select(col("value").as("name"))
      .groupBy(col("name")) // RelationalGroupedDataset
      .count()

    names
      .writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregation without watermark
      .start()
      .awaitTermination()

  }

}
