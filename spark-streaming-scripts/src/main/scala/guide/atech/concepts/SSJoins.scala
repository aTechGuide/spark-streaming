package guide.atech.concepts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SSJoins {


  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private val guitarPlayers = spark.read
      .option("inferSchema", value = true)
    .json("src/main/resources/data/guitarPlayers")

  private val guitars = spark.read
    .option("inferSchema", value = true)
    .json("src/main/resources/data/guitars")

  private val bands = spark.read
    .option("inferSchema", value = true)
    .json("src/main/resources/data/bands")

  private def readFromSocket(port: Int) = {
    spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", port)
      .load()

  }


  private def joinStreamWithStatic = {

    val streamedBandsDF = readFromSocket(12345)
      .select(from_json(col("value"), bands.schema).as("band")) // Returns a composite column
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // JOIN Happens Per Batch
    val streamedBandGuitaristsDF = streamedBandsDF.join(guitarPlayers, guitarPlayers.col("band") ===
      streamedBandsDF.col("id"), "inner")

    // Restrictions (As Spark does NOT allow unbounded accumulation of data)
    /*
      When Streaming is joined with static
      - right outer join, full outer join and right semi / anti join are not permitted

      When static is joined with Streaming
      - LEFT outer join, FULL outer join and LEFT semi / anti join are not permitted
     */

    streamedBandGuitaristsDF
  }


  private def joinStreamWithStream = {

    val streamedBandsDF = readFromSocket(12345)
      .select(from_json(col("value"), bands.schema).as("band")) // Returns a composite column
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")


    val streamedGuitaristDF = readFromSocket(12346)
      .select(from_json(col("value"), guitarPlayers.schema).as("guitarPlayer")) // Returns a composite column
      .selectExpr("guitarPlayer.id as id", "guitarPlayer.name as name", "guitarPlayer.guitars as guitars", "guitarPlayer.band as band")

    // Joining Stream with Stream
    val streamedJoin = streamedBandsDF.join(streamedGuitaristDF, streamedGuitaristDF.col("band") ===
      streamedBandsDF.col("id"))

    /*
      - inner joins are supported
      - LEFT and RIGHT outer joins are supported, but Must have watermarks
      - FULL outer joins are not supported
     */

    streamedJoin

  }

  def main(args: Array[String]): Unit = {

    // Joining static DFs
    val joinCondition = guitarPlayers.col("band") === bands.col("id")

    // val guitaristsBand = guitarPlayers.join(bands, joinCondition, "inner")

    //val joined = joinStreamWithStatic

    val joined = joinStreamWithStream

    joined
      .writeStream
      .format("console")
      .outputMode("append") // Only Append supported for Stream Vs Stream Joining
      .start()
      .awaitTermination()

  }

}
