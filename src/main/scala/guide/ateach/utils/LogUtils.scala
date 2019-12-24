package guide.ateach.utils

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object LogUtils {
  def logAlarmer(lines: ReceiverInputDStream[String]) = {

    val pattern = apacheLogPattern()
    val statuses = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) matcher.group(6) else "[error]"
    })

    val successFailure = statuses.map(x => {
      val statusCode = util.Try(x.toInt) getOrElse 0
      if (statusCode >= 200 && statusCode < 300) {
        "Success"
      } else if (statusCode >= 500 && statusCode < 600) {
        "Failure"
      } else {
        "Other"
      }
    })

    val statusCounts = successFailure.countByValueAndWindow(Seconds(300), Seconds(1))

    statusCounts.foreachRDD((rdd, _) => {

      var totalSuccess:Long = 0
      var totalError:Long = 0

      if (rdd.count() > 0) {
        val elements = rdd.collect()
        for (element <- elements) {
          val result = element._1
          val count = element._2

          if (result == "Success") {
            totalSuccess += count
          }

          if (result == "Failure") {
            totalError += count
          }
        }
      }

      println(s"Total Success: $totalSuccess and Failure: $totalError")

      if (totalError + totalSuccess > 100) {

        val ratio:Double = util.Try(totalError.toDouble / totalSuccess.toDouble) getOrElse 1.0

        if (ratio > .5) {
          println("ALARM: Something is wrong")
        } else {
          println("All systems go.")
        }
      }

    })

  }


  def trackTopURL(lines: ReceiverInputDStream[String]) = {

    val pattern = apacheLogPattern()
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})
    val urls = requests.map(x => {val arr = x.toString.split(" "); if (arr.size == 3) arr(1) else "[error]"})
    val urlCounts = urls.map(x => (x,1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(500), Seconds(1))

    urlCounts.transform(rdd => rdd.sortBy(x => x._2, false )).print()
  }

  def apacheLogPattern():Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }

}
