package guide.ateach.sample

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val words = sc.textFile("./book.txt").flatMap(line => line.split(" ")).map(word => word.toLowerCase())
    val count = words.countByValue()
    for ((word, count) <- count.take(10)) {
      println(word + " " + count)
    }

    val env = args(0)
    val externalConfig = args(1)

    val config =
      if (env == "PROD") ConfigFactory.parseFile(new File(externalConfig)).withFallback(ConfigFactory.load("prod.json")).withFallback(ConfigFactory.load())
      else ConfigFactory.parseFile(new File(externalConfig)).withFallback(ConfigFactory.load())

    println("app.debug = " + config.getBoolean("app.debug"))
    println("spark.url = " + config.getString("spark.url"))
    println("spark.name = " + config.getString("spark.name"))

//    println("system Properties = " + ConfigFactory.systemProperties())
//    println("system Environment = " + ConfigFactory.systemEnvironment())

    sc.stop()
  }
}
