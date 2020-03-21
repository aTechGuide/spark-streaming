package guide.atech.jdbc

import guide.atech.schema.{Car, CarSchema}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object SSPostgres {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/atechguide"
  val user = "docker"
  val password = "docker"

  import spark.implicits._

  def writeStreamToPostgres() = {

    val carsDF = spark
      .readStream
      .schema(CarSchema.schema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>

        // each executor can control the batch
        // Batch is a static Dataset/ dataframe

        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars")
          .mode(SaveMode.Overwrite) // SaveMode.Append
          .save()

      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    writeStreamToPostgres()
  }

}
