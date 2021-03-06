package guide.atech.kafka

import guide.atech.schema.Car
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Ref Docs: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
  *
  * Schema Properties
  * - key -> determines which partition value will fed into
  * - value -> binary representation of string (basically binary representation of any character)
  * - timestamp -> Moment in time when I added string into kafka topic
  */

object SSKafkaIntegration {

  val topic = "atechguide_first_topic"
  val jsonTopic = "atechguide_car_json"
  val startingOffset = "earliest"

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  /**
   * Sample of what's read from Kafka
   *  - [Without expr("cast(value as string) as actualValue")]
   *
   * +----------+--------------------+--------------------+---------+------+--------------------+-------------+
   * |       key|               value|               topic|partition|offset|           timestamp|timestampType|
   * +----------+--------------------+--------------------+---------+------+--------------------+-------------+
   * |[4B 65 79]|[56 61 6C 75 65 2...|atechguide_first_...|        0|     8|2020-04-06 16:56:...|            0|
   * +----------+--------------------+--------------------+---------+------+--------------------+-------------+
   *
   * - [With expr("cast(key as string) as key") , expr("cast(value as string) as actualValue")]
   *
   * +-----+-----------+--------------------+
   * |  key|actualValue|               topic|
   * +-----+-----------+--------------------+
   * |key 2|    Value 2|atechguide_first_...|
   * +-----+-----------+--------------------+
   */
  private def readFromKafka(): Unit = {

    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", startingOffset)
      .option("subscribe", topic)
      .load()


    kafkaDF
      .select(expr("cast(key as string) as key") , expr("cast(value as string) as actualValue"), col("topic"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  private def writeToKafka(): Unit = {

    val carsDF = spark
      .readStream
      .schema(Car.carsSchema)
      .json("src/main/resources/data/cars")

    // Make DF Kafka Compatible by creating a key and value
    val carsKafkaDF = carsDF
      .selectExpr("upper(Name) as key", "Name as value")

    carsKafkaDF
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "atechguide")
      .option("checkpointLocation", "checkpoint") // without checkpoint writing will fail.
      // In case of failure checkpointLocation marks what data has already sent to kafka
      .start()
      .awaitTermination()
  }

  private def writeJsonToKafka(): Unit = {

    val carsDF = spark
      .readStream
      .schema(Car.carsSchema)
      .json("src/main/resources/data/cars")

    val carsJsonKafkaDF = carsDF
      .select(
        col("Name").as("key"),
        to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("String").as("value")
      ).limit(3)

    carsJsonKafkaDF
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", jsonTopic)
      .option("checkpointLocation", "checkpoint")
      .start()
      .awaitTermination()

  }

  private def readJsonToKafka(): Unit = {


    val carsData = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", startingOffset)
      .option("subscribe", jsonTopic)
      .load()

    val schema = StructType(Seq(
      StructField("Name", StringType),
      StructField("Horsepower", StringType),
      StructField("Origin", StringType)
    ))

    /**
     *
     * +--------------------+--------------------+--------------------+----------+------+
     * |                 key|                data|                Name|Horsepower|Origin|
     * +--------------------+--------------------+--------------------+----------+------+
     * |chevrolet chevell...|[chevrolet chevel...|chevrolet chevell...|       130|   USA|
     * |   buick skylark 320|[buick skylark 32...|   buick skylark 320|       165|   USA|
     * |  plymouth satellite|[plymouth satelli...|  plymouth satellite|       150|   USA|
     * +--------------------+--------------------+--------------------+----------+------+
     */
    carsData
      .select(expr("cast(key as string) as key"), expr("cast(value as string) as value"), col("topic"))
      .select(col("key"), from_json(col("value"), schema).as("data"))
      .select("key", "data", "data.*")
      .writeStream
      .format("console")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {

     //readFromKafka()
    //writeToKafka()
    readJsonToKafka()

  }

}
