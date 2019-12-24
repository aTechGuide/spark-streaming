package guide.ateach.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import guide.ateach.utils.LogUtils

object LogParser extends App {

  val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))
  ssc.sparkContext.setLogLevel("ERROR")

  val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

  //LogUtils.trackTopURL(lines)
  LogUtils.logAlarmer(lines)

  ssc.checkpoint("/Users/kamali/mcode/tmp/")
  ssc.start()
  ssc.awaitTermination()


}
