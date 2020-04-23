package guide.atech.sources.files.socket

import java.net.Socket

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.{Future, Promise}
import scala.io.Source

// In prod use "MEMORY_AND_DISK_2" as it will save data in both memory and disk and replicated twice
class CustomSocketReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  import scala.concurrent.ExecutionContext.Implicits.global

  val socketPromise: Promise[Socket] = Promise[Socket]()
  val socketFuture = socketPromise.future

  // onStart is called asynchronously
  override def onStart(): Unit = {
    val socket = new Socket(host, port)

    // run on another thread
    Future {
      Source.fromInputStream(socket.getInputStream)
        .getLines()
        .foreach(line => store(line)) // store make the string available to spark
    }

    socketPromise.success(socket)
  }

  // onStop is called asynchronously
  override def onStop(): Unit = socketFuture.foreach(socket => socket.close())
}

object TweetReceiver {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def main(args: Array[String]): Unit = {

    val dataStream: DStream[String] = ssc.receiverStream(new CustomSocketReceiver("localhost", 12345))
    dataStream.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
