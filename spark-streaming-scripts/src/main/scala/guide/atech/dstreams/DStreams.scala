package guide.atech.dstreams

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

import guide.atech.schema.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreams {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /*
    Spark StreamingContext is entery point to D Streams API
    - Needs a spark context and batch interval
   */
  private val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
   - Define Input sources by creating a Dstreams
   - Define transformations on DStreams
   - Call an action on DStreams
   - Trigger to start ALL computation with ssc.start()
     - after this point no more computations can be added
   - await termination or stop the computation
   - We can not restart a computation
   */

  private def readFromSocket(): Unit = {

    val socketSteams = ssc
      .socketTextStream("localhost", 12345)

    // transformation: lazy
    val wordStreams: DStream[String] = socketSteams
      .flatMap(line => line.split(" "))

    // action
    // wordStreams.print()
    wordStreams.saveAsTextFiles("src/main/resources/data/words") // Each folder is an RDD = batch, each file is a partion of RDD

    ssc.start()
    ssc.awaitTermination()

  }

  def createNewFile(): Unit = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val directory = new File(path) // directory where I will store new file
      val nFiles = directory.listFiles().length

      val newFile = new File(s"$path/newStock$nFiles.csv")

      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Feb 1 2001,9.12
          |AAPL,Mar 1 2001,11.03
          |AAPL,Apr 1 2001,12.74
          |AAPL,May 1 2001,9.98
          |AAPL,Jun 1 2001,11.62
          |AAPL,Jul 1 2001,9.4
          |AAPL,Aug 1 2001,9.27
        """.stripMargin.trim)

      writer.close()

    }).start()
  }

  private def readFromFile(): Unit = {

    createNewFile() // operates on another thread

    /*
      ssc.textFileStream => Monitors the directory for new files
     */
    val textStream: DStream[String] = ssc
      .textFileStream("src/main/resources/data/stocks")

    // transformations
    val dateFormat = new SimpleDateFormat("MMM d yyy")

    val stocks: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    // action
    stocks.print()

    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {

   readFromSocket()

   // readFromFile()



  }

}
