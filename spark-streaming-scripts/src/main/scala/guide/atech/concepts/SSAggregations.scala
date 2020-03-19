package guide.atech.concepts

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
  *
  * Aggregations work at micro batch level.
  * Aggregation will run when a new batch is created
  * Not Supported Aggregations
  * - distinct, sorting, multiple aggregations in same structure (Otherwise spark will need to keep track of EVERYTHING (unbounded data))
  */
object SSAggregations {

  private val spark = SparkSession.builder()
    .appName("SSAggregations")
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

  private def countLines(lines: DataFrame) = {
    val lineCount = lines.selectExpr("count(*) as lineCount")

    lineCount
      .writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregation without watermark
      .start()
      .awaitTermination()
  }


  def numericAggregation(lines: DataFrame, aggFunction: Column => Column): Unit = {

    val numbers = lines.select(col("value").cast(IntegerType).as("number"))
    val aggregationDF = numbers.select(aggFunction(col("number")).as("aggeration"))

    aggregationDF
      .writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregation without watermark
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {


    val lines = readFromSocket
    // countLines(lines)

    numericAggregation(lines, sum)

  }


}
