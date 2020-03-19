package guide.atech.concepts

import guide.atech.schema.{Car, common}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}

/**
  * Type Safe Structured Streams
  *
  * Pros
  * - Type safety and expressiveness
  *
  * Downsides
  * - Potential Perf implications as lamdas can not be optimised
  */
object SSDatasets {

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

  def main(args: Array[String]): Unit = {

    // Option 1
    // Include Encoders for df - ds transformation
    import spark.implicits._

    //  Options 2
    // val carEncoder = Encoders.product[Car]

    val carsDS: Dataset[Car] = readFromSocket
      .select(from_json(col("value"), common.carsSchema).as("car")) // Returns a composite column (struct)
      .selectExpr("car.*") // DF
      .as[Car]

    // Transformations

    val carNameDF: DataFrame = carsDS.select(col("Name")) // DF

    val carNameDS: Dataset[String] = carsDS.map(_.Name) // DS

    carNameDS
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

}
