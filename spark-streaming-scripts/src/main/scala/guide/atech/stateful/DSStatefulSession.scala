package guide.atech.stateful

import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import java.util.regex.Matcher

import guide.atech.sources.files.socket.LogUtils

/**
  * Reference => https://www.udemy.com/course/taming-big-data-with-spark-streaming-hands-on
  */

object DSStatefulSession {

    /**
      * It contains a session length and a list of URL's visited (as clickstream:List[String])
      */
    case class SessionData(sessionLength: Long, var clickstream:List[String])

    /** This function gets called as new data is streamed in, and maintains state across whatever key you define.
      * In this case, it expects to get an IP address as the key (we maintain session via key), a String as a URL (wrapped in an Option to handle exceptions), and
      * maintains state defined by our SessionData class defined above.
      *
      * Its output is new key, value pairs of IP address and the updated SessionData that takes this new line into account.
      */
    def trackStateFunc(batchTime: Time, ip: String, url: Option[String], state: State[SessionData]): Option[(String, SessionData)] = {
      // Extract the previous state passed in (using getOrElse to handle exceptions)
      val previousState = state.getOption.getOrElse(SessionData(0, List()))

      // Create a new state that increments the session length by one, adds this URL to the click stream, and clamps the click stream
      // list to 10 items
      val newState = SessionData(previousState.sessionLength + 1L, (previousState.clickstream :+ url.getOrElse("empty")).take(10))

      // Update our state with the new state.
      state.update(newState)

      // Return a new key/value result.
      Some((ip, newState))
    }

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "Sessionizer", Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

    private def extractDataFromLogs = {
      // Construct a regular expression (regex) to extract fields from raw Apache log lines
      val pattern = LogUtils.apacheLogPattern()

      // Create a socket stream to read log data published via netcat on port 9999 locally
      val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

      // Extract the (ip, url) we want from each log line
      val requests = lines.map(x => {
        val matcher: Matcher = pattern.matcher(x)
        if (matcher.matches()) {
          val ip = matcher.group(1)
          val request = matcher.group(5)
          val requestFields = request.toString.split(" ")
          val url = scala.util.Try(requestFields(1)) getOrElse "[error]"
          (ip, url)
        } else {
          ("error", "error")
        }
      })
      requests
    }


    def main(args: Array[String]) {

      // Extracting the (ip, url) from each log line
      val requests = extractDataFromLogs

      // We'll define our state using our trackStateFunc function above, and also specify a
      // session timeout value of 30 minutes.
      /**
        * timeout(Minutes(30)) => Set the duration after which the state of an idle key will be removed.
        *
        * A key and its state is considered idle
        *   if it has not received any data for at least the given duration.
        *
        * The mapping function will be called one final time on the idle states that are going to be removed;
        * State.isTimingOut() set to true in that call.
        */

      val stateSpec = StateSpec.function(trackStateFunc _).timeout(Minutes(30))

      // Now we will process this data through our StateSpec to update the stateful session data
      // Note that our incoming RDD contains key/value pairs of ip/URL, and that what our
      // trackStateFunc above expects as input.
      val requestsWithState = requests.mapWithState(stateSpec)

      // And we'll take a snapshot of the current state so we can look at it.
      val stateSnapshotStream = requestsWithState.stateSnapshots()

      // Process each RDD from each batch as it comes in
      stateSnapshotStream.foreachRDD((rdd, time) => {

        // We'll expose the state data as SparkSQL, but you could update some external DB
        // in the real world.

        val spark = SparkSession
          .builder()
          .appName("Sessionizer")
          .getOrCreate()

        import spark.implicits._

        // Slightly different syntax here from our earlier SparkSQL example. toDF can take a list
        // of column names, and if the number of columns matches what's in your RDD, it just works
        // without having to use an intermediate case class to define your records.
        // Our RDD contains key/value pairs of IP address to SessionData objects (the output from
        // trackStateFunc), so we first split it into 3 columns using map().
        val requestsDataFrame = rdd.map(x => (x._1, x._2.sessionLength, x._2.clickstream))
          .toDF("ip", "sessionLength", "clickstream")

        // Create a SQL table from this DataFrame
        requestsDataFrame.createOrReplaceTempView("sessionData")

        // Dump out the results - you can do any SQL you want here.
        val sessionsDataFrame =
          spark.sqlContext.sql("select * from sessionData")
        println(s"========= $time =========")
        sessionsDataFrame.show()

      })

      // Kick it off
      ssc.checkpoint("checkpoint")
      ssc.start()
      ssc.awaitTermination()
    }
}
