package guide.atech.kafka

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * nc -lk 12345
  */
object DSKafkaIntegration {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer], // Serialize bytes from spark to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // Receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "atechguide"

  def readFromKafka() = {
    val topics = Array(kafkaTopic)

    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      /*
        distribute the partitions evenly across the spark cluster
        Alternatives
        - PreferBrokers: If brokers and executors are in the same cluster
        - PreferFixed
       */
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
      /*
        Every Stream needs group.id to satisfy kafka cache requirements
        Alternative
        - SubscribePattern -> Allows to pass regex to match topic names
        - Assign - Allows specifying offsets and partitions per Topic
       */
    )

    val processedStream = kafkaDStream
      .map( record => (record.key(), record.value()))

    processedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka() = {
    val inputData = ssc
      .socketTextStream("localhost", 12345)

    val data = inputData.map(_.toUpperCase())

    data.foreachRDD {rdd =>
      rdd.foreachPartition{ partition =>
        // Inside this lambda code is run by a single executor
        // Each partition resides on a single executor

        val kafkaHashMap = new util.HashMap[String, Object]()
        kafkaParams.foreach { pair =>
          kafkaHashMap.put(pair._1, pair._2)
        }

        // Producer can insert records into kafka topic
        // available on this executor
        // We can not move this producer outside `foreachPartition` to inside `foreachRDD` because then driver has to send this producer
        // to each executor (by serializing it) which is impossible
        val producer = new KafkaProducer[String, String](kafkaHashMap)

        partition.foreach { value =>
          val message = new ProducerRecord[String, String](kafkaTopic, null, value)

          // feed message into kafka topic
          producer.send(message)
        }

        producer.close()

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    // readFromKafka()
    writeToKafka()

  }

}
