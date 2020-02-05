package guide.ateach.kafka


import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

import java.util.regex.Matcher

import guide.ateach.utils.LogUtils

/**
 * Kafka Commands
 * - Create Topic
 *   - kafka-topics.sh --bootstrap-server localhost:9092 --topic testLogs --create --partitions 1 --replication-factor 1
 * - Add Data via Producer
 *   - kafka-console-producer.sh --broker-list localhost:9092 --topic testLogs < /Users/kamali/mcode/SparkStreaming/log/access_log.txt
 */
object KafkaStreamConsumer extends App {

  val ssc = new StreamingContext("local[*]", "KafkaConsumer", Seconds(1))
  ssc.sparkContext.setLogLevel("ERROR")

  val pattern = LogUtils.apacheLogPattern()

  val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")

  val topics = List("testLogs").toSet // <- List of Topics to listen to

  val logLines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)

  val requests = logLines.map( x => {
    val matcher:Matcher = pattern.matcher(x)
    if (matcher.matches()) matcher.group(5)
  })

  val urls = requests.map(x => {val arr = x.toString.split(" "); if (arr.size == 3) arr(1) else "[error]"})
  val urlCounts = urls.map(x => (x,1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(500), Seconds(1))

  urlCounts.transform(rdd => rdd.sortBy(x => x._2, false )).print()

  ssc.checkpoint("/Users/kamali/mcode/checkpoint")
  ssc.start()
  ssc.awaitTermination()

}
