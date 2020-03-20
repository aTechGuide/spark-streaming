package guide.atech.dstreams

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

import guide.atech.schema.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * cat people-1m.txt | nc -lk 12345
  */
object DSTransformations {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readPeople = {
    ssc.socketTextStream("localhost",12345).map { line =>
      val tokens = line.split(":")
      Person (
        tokens(0).toInt,
        tokens(1),
        tokens(2),
        tokens(3),
        tokens(4),
        Date.valueOf(tokens(5)),
        tokens(6),
        tokens(7).toInt
      )
    }
  }

  // map transformation
  def peoplesAge() = readPeople.map { person =>
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears

    (s"${person.firstName} ${person.lastName}", age)
  }

  // flatMap
  def peopleSmallNames() = readPeople.flatMap { person =>
    List(person.firstName, person.middleName)
  }

  // filter
  def highIncomePeople() = readPeople.filter { person => person.salary > 80000}

  // count
  def countPeople: DStream[Long] = readPeople.count() // Number of entries in every batch

  // count by value, PER Batch
  def countNames() = readPeople.map(_.firstName).countByValue()

  /*
    Reduce by key
    - Works on DStrems of Tuples
    - Works per batch
   */
  def countNamesReduce = readPeople
    .map(_.firstName)
    .map(name => (name, 1))
    .reduceByKey(_ + _)


  // Processing each RDD independently
  def saveToJson = readPeople.foreachRDD{ rdd =>

    import spark.implicits._

    // dataset per batch
    val ds = spark.createDataset(rdd)
    val file = new File("src/main/resources/data/people")

    val nFiles = file.listFiles().length
    val path = s"src/main/resources/data/people/people$nFiles.json"

    ds.write.json(path)
  }



  def main(args: Array[String]): Unit = {

//    val streams = countNamesReduce
//    streams.print()

    saveToJson

    ssc.start()
    ssc.awaitTermination()
  }

}
