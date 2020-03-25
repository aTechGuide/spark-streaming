package guide.atech.socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Open a socket and push log file into it using following
  * nc -kl 9999 -i 1 < log/access_log.txt
  */
object LogParser extends App {

  val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))
  ssc.sparkContext.setLogLevel("ERROR")

  val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

  //LogUtils.trackTopURL(lines)
  //LogUtils.logAlarmer(lines)
  LogUtils.logSQL(lines)

  ssc.checkpoint("checkpoint")
  ssc.start()
  ssc.awaitTermination()


}
