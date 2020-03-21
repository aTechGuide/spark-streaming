package guide.atech.dstreams

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object DSWindowTransformation {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private val ssc = new StreamingContext(spark.sparkContext, Seconds(1))


  private def readFromSocket() = ssc.socketTextStream("localhost", 12345)


  /*
     window = keeps all the data between "now and X time back"
     window interval is updated with every batch

     Also, Window interval must be multiple of batch interval
   */
  private def linesByWindow() = readFromSocket().window(Seconds(10))


  /*
    - We Consider past 10s data
    - Sliding interval is 5s (Not 1s as before). So batch interval is 5s now
    - Both arguments must be a multiple of original batch duration
   */
  private def linesBySlidingWindow() = readFromSocket().window(Seconds(10) /*Window Interval*/, Seconds(5) /*Sliding Interval*/)


  // Count number if elements over a window
  // 60m window updated evey 30 secs
  private def countLinesByWindow =
    // readFromSocket().window(Minutes(60), Seconds(30)).count() // Method 1
    readFromSocket().countByWindow(Minutes(60), Seconds(30)) // Method 2

  // aggregate data in different way [Method 1]
  private def sumAllTextByWindow = readFromSocket().map(_.length).window(Seconds(10), Seconds(5))
    .reduce(_ + _)

    // [Method 2]
  private def sumAllTextByWindowAlt = readFromSocket().map(_.length).reduceByWindow(_ + _, Seconds(10), Seconds(5))

  // tumbling windows: No Slide interval and window duration is updated every window duration
  private def linesByTumblingWindow = readFromSocket().window(Seconds(10), Seconds(10)) // batch of batches


  private def computeWordOccurencesByWindow() = {

    // For reduceByKeyAndWindow we need a checkpoint dir
    ssc.checkpoint("checkpoint") // checkpoints are created at the root of the folder
    readFromSocket()
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow(
        _ + _, // Reduction Function
        _ - _, // Inverse Function = when old tuples are falling out of window the values that needs to be counted out
        Seconds(60),  // Window duration
        Seconds(30) // sliding Duration
      )
  }

  /**
    * Exercise.
    * Word longer than 10 chars => $2
    * Every other word => $0
    *
    * Input text into the terminal => money made over the past 30 seconds, updated every 10 seconds.
    * - use window
    * - use countByWindow
    * - use reduceByWindow
    * - use reduceByKeyAndWindow
    */

  val moneyPerExpensiveWord = 2

  def showMeMoney = readFromSocket()
    .flatMap(_.split(" "))
    .filter(_.length > 10)
    .map(_ => moneyPerExpensiveWord)
    .reduce(_ + _) // Optimizations later which will be useful. Even if we do NOT do it here, final result will be same
    .window(Seconds(30), Seconds(10))
    .reduce(_ + _)

  def showMeMoney2 = readFromSocket()
    .flatMap(_.split(" "))
    .filter(_.length > 10)
    .countByWindow(Seconds(30), Seconds(10)) // Number of expensive words
    .map(_ * moneyPerExpensiveWord)

  def showMeMoney3 = readFromSocket()
    .flatMap(_.split(" "))
    .filter(_.length > 10)
    .map(_ => moneyPerExpensiveWord)
    .reduceByWindow(_ + _, Seconds(30), Seconds(10))

  def showMeMoney4 = {

    ssc.checkpoint("checkpoint")

    readFromSocket()
      .flatMap(_.split(" "))
      .filter(_.length > 10)
      .map(_ => ("expensive", moneyPerExpensiveWord))
      .reduceByKeyAndWindow(
        _ + _, // Reduction Function
        _ - _, // Inverse Function
        Seconds(30),
        Seconds(10)
      )
  }




  def main(args: Array[String]): Unit = {

    computeWordOccurencesByWindow()
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
