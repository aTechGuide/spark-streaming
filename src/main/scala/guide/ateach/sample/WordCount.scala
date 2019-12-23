package guide.ateach.sample

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {

  val conf = new SparkConf().setAppName("WordCount")
  conf.setMaster("local[*]")
  val sc = new SparkContext(conf)

  val words = sc.textFile("./book.txt").flatMap(line => line.split(" ")).map(word => word.toLowerCase())

  val count = words.countByValue()

//  count.take(20).foreach(data => (value, count))

  for ((word, count) <- count.take(10)) {
    println(word + " " + count)
  }

  sc.stop()
}
