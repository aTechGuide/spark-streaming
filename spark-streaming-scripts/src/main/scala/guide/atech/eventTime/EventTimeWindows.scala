package guide.atech.eventTime

import guide.atech.schema.common
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * For window functions, windows start at Jan 1 1970, 12 AM GMT
  *
  * We will look into how to operate a window function on a column on event time
  *
  * Remember
  *  - Window duration and sliding interval must be a multiple of the batch interval
  */
object EventTimeWindows {

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
      .select(from_json(col("value"), common.onlinePurchaseSchema).as("purchase")) // composite column
      .selectExpr("purchase.*")

  }

  private def readFromFile = {

    spark
      .readStream
      .schema(common.onlinePurchaseSchema)
      .json("src/main/resources/data/purchases")

  }

  def aggregatePurchasesBySlidingWindow = {
    val purchasesDF = readFromSocket
    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // struct column: has fields {start and end}
      .agg(sum("quantity").as("totalQuantity"))
      .selectExpr("time.*", "totalQuantity")

    windowByDay
      .writeStream
      .option("numRows", 50)
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregatePurchasesByTumblingWindow = {
    val purchasesDF = readFromSocket
    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time")) // tumbling window: sliding duration == window duration
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay
      .writeStream
      .option("numRows",50)
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // Bestselling product of every dat and quantity sold
  def bestSellingProductPerDay(): Unit = {
    val purchasesDF = readFromFile
    val windowByDay = purchasesDF
      .groupBy(col("item"), window(col("time"), "1 day").as("day")) // Tumbling window
      .agg(sum("quantity").as("totalQuantity"))
      .selectExpr("day.*", "item", "totalQuantity")
      .orderBy(col("day"), col("totalQuantity").desc)


    windowByDay
      .writeStream
      .option("numRows", 50)
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // Best selling product of 24 hours, updated every hour
  def bestSellingProductEvery24h(): Unit = {
    val purchasesDF = readFromFile
    val windowByDay = purchasesDF
      .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("time")) // Tumbling window
      .agg(sum("quantity").as("totalQuantity"))
      .selectExpr("time.*", "item", "totalQuantity")
      .orderBy(col("start"), col("totalQuantity").desc)


    windowByDay
      .writeStream
      .option("numRows", 50)
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // aggregatePurchasesBySlidingWindow
    // bestSellingProductPerDay
    bestSellingProductEvery24h

  }

}
