package guide.atech.files

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.{Matcher, Pattern}

import guide.atech.socket.LogUtils
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.{Row, SparkSession}

/**
  * This script creates stream directly from log folder
  */

object SSLogs extends App {

  case class LogEntry(ip:String, client:String, user:String, dateTime:String, request:String, status:String, bytes:String, referer:String, agent:String)

  val logPattern = LogUtils.apacheLogPattern()
  val datePattern = Pattern.compile("\\[(.*?) .+]")

  // Function to convert Apache log times to what Spark/SQL expects
  def parseDateField(field: String): Option[String] = {

    val dateMatcher = datePattern.matcher(field)
    if (dateMatcher.find) {
      val dateString = dateMatcher.group(1)
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val date = dateFormat.parse(dateString)
      val timestamp = new java.sql.Timestamp(date.getTime)
      Option(timestamp.toString)
    } else {
      None
    }
  }

  def parseLog(x:Row) : Option[LogEntry] = {

    val matcher:Matcher = logPattern.matcher(x.getString(0))
    if (matcher.matches()) {
      val timeString = matcher.group(4)
      Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateField(matcher.group(4)).getOrElse(""),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)
      ))
    } else {
      None
    }
  }

  val spark = SparkSession
    .builder()
    .appName("Structured Streaming Logs")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val logs = spark.readStream.text("log")

  import spark.implicits._

  val structuredLogs = logs.flatMap(parseLog).select("status", "dateTime") //<- We can define the info that we want to use for the window

  // We are using "dateTime" column to define the sliding 1 hour window
  val windowed = structuredLogs.groupBy($"status", window($"dateTime", "1 hour")).count().orderBy("window")

  // Use "complete" output mode because we are aggregating (instead of "append").
  val query = windowed.writeStream.outputMode("complete").format("console").start()

  query.awaitTermination()
  spark.stop()
}
