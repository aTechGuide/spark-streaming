package guide.atech.testing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}

case class Event(userId: Int, offset: Int)
object MemoryStreamTesting {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  private def testingEvent: Unit = {

    implicit val ctx = spark.sqlContext
    import spark.implicits._

    // It uses two implicits: Encoder[Int] and SQLContext
    val events = MemoryStream[Event]
    val sessions = events.toDS
    assert(sessions.isStreaming, "sessions must be a streaming Dataset")

    // use sessions event stream to apply required transformations
    val transformedSessions = sessions

    val queryName = "TestingTable"
    val streamingQuery = transformedSessions
      .writeStream
      .format("memory")
      .queryName(queryName)
      //.option("checkpointLocation", "checkpoint")
      .outputMode("append")
      .start()

    // Add events to MemoryStream as if they came from Kafka
    val batch = Seq(
      Event(userId = 1, offset = 1 ),
      Event(userId = 2, offset = 2 ))

    val currentOffset = events.addData(batch)
    streamingQuery.processAllAvailable()
    events.commit(currentOffset.asInstanceOf[LongOffset])

    // check the output
    // The output is in queryName table
    // The following code simply shows the result
    spark
      .table(queryName)
      .show(truncate = false)
  }

  private def testingInt(): Unit = {

    implicit val ctx = spark.sqlContext
    import spark.implicits._

    // It uses two implicits: Encoder[Int] and SQLContext
    val intsInput = MemoryStream[Int]

    val queryName = "memStream"

    val memoryQuery = intsInput.toDF
      .writeStream
      .format("memory")
      .queryName(queryName)
      .start

    intsInput.addData(0, 1, 2)

    memoryQuery.processAllAvailable()

    spark
      .table("memStream")
      .as[Int]
      .show(false)

  }

  private def testingIntConsole(): Unit = {

    implicit val ctx = spark.sqlContext
    import spark.implicits._

    // It uses two implicits: Encoder[Int] and SQLContext
    val intsInput = MemoryStream[Int]

    val memoryQuery = intsInput.toDF
      .writeStream
      .format("console")
      .start

    intsInput.addData(0, 1, 2)

    memoryQuery.processAllAvailable()
  }

  def main(args: Array[String]): Unit = {
    testingIntConsole()

  }

}
