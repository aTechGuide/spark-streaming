package guide.atech.cassandra

import java.util.regex.Matcher

import com.datastax.spark.connector._
import guide.atech.sources.files.socket.LogUtils
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create Table CREATE TABLE cassandra.log (ip text PRIMARY KEY, status int, url text, useragent text);
  */
object DSCassandraOutput extends App {

  var conf = new SparkConf()
  conf.set("spark.cassandra.connection.host", "127.0.0.1")
  conf.setMaster("local[*]")
  conf.setAppName("Cassandra Output")

  val ssc = new StreamingContext(conf, Seconds(10))
  ssc.sparkContext.setLogLevel("ERROR")

  val pattern = LogUtils.apacheLogPattern()

  val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

  val requests = lines.map( x => {
    val matcher: Matcher = pattern.matcher(x)

    if (matcher.matches()) {
      val ip = matcher.group(1)
      val request = matcher.group(5)
      val requestFields = request.toString.split(" ")
      val url = scala.util.Try(requestFields(1)) getOrElse "[error]"
      (ip, matcher.group(6).toInt, url, matcher.group(9))
    } else {
      ("error", 0, "error", "error")
    }
  })

  requests.foreachRDD((rdd, _) => {
    rdd.cache()
    println("Writing " + rdd.count() + " rows to Cassandra")
    rdd.saveToCassandra("cassandra", "log", SomeColumns("ip", "status", "url", "useragent"))
  })

  ssc.checkpoint("checkpoint")
  ssc.start()
  ssc.awaitTermination()

}
