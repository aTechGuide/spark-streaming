package guide.atech.nosql

import com.datastax.spark.connector.cql.CassandraConnector
import guide.atech.schema.{Car, CarSchema}
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._ // cassandra Connector library
object SSCassandraIntegration {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def writeStreamToCassandraInBatches() = {

    import spark.implicits._

    val carsDS = spark
      .readStream
      .schema(CarSchema.schema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch{ (batch: Dataset[Car], batchID: Long) =>

        // Save this batch to Cassandra in a single table write
        batch
          .select(col("Name"), col("Horsepower"))
          .write
          .cassandraFormat("cars", "public") // Type Enrichment
          .mode(SaveMode.Append)
          .save()

      }
      .start()
      .awaitTermination()
  }

  class CarCassandraForeachWriter extends ForeachWriter[Car] {

    /*
      - On every batch, on every partition `partitionId`
        - on every "epoch" = chunk of data
          - call open method; if false, skip this chunk
          - for each entry in this chunk, call process method
          - call close method either at the end of the chunk or with an error if it was thorwn
     */

    val keyspace = "public"
    val table = "cars"
    val connector = CassandraConnector(spark.sparkContext.getConf)

    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("Open Connection")
      true
    }

    override def process(car: Car): Unit = {
      connector.withSessionDo {session =>
        session.execute(
          s"""
             |insert into $keyspace.$table("Name", "Horsepower")
             |values ('${car.Name}', ${car.Horsepower.orNull})
           """.stripMargin)
      }
    }

    override def close(errorOrNull: Throwable): Unit = println("Closing Connection")
  }

  def writeStreamToCassandra() = {

    import spark.implicits._

    val carsDS = spark
      .readStream
      .schema(CarSchema.schema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreach(new CarCassandraForeachWriter)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // writeStreamToCassandraInBatches()
    writeStreamToCassandra()

  }

}


