package guide.ateach.utils

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

object TweetUtils {
  def mostPopularHashtag(tweets: DStream[String]) = {

    tweets
      .flatMap(tweet => tweet.split(" "))
      .filter(word => word.startsWith("#"))
      .map(hashtag => (hashtag, 1))
      .reduceByKeyAndWindow((x,y) => x+y, (x,y) => x-y, Seconds(300), Seconds(1)) //<- Reducing on 5m window sliding every 1 sec
      .transform(rdd => rdd.sortBy(x => x._2,false))
      .print(10)

  }

  def averageTweetLength(tweets: DStream[String]) = {
    val lengths = tweets.map(tweet => tweet.length())

    val totalTweets = new AtomicLong(0)
    val totalChars = new AtomicLong(0)

    lengths.foreachRDD((rdd, _) => {

      val count = rdd.count()
      if (count > 0) {
        totalTweets.getAndAdd(count)

        totalChars.getAndAdd(rdd.reduce((x,y) => x+y))

        println(s"Total Tweets: ${totalTweets.get()}, Total Chars = ${totalChars.get()}, Average = ${totalChars.get()/totalTweets.get()}")
      }
    })
  }

  def saveTweets(statuses: DStream[String]) = {

    statuses.foreachRDD((rdd, time) => {

      if (rdd.count() > 0) {
        //combine all RDDs
        val repartitionedRDD = rdd.repartition(1).cache()
        repartitionedRDD.saveAsTextFile("Tweets_" + time.milliseconds.toString)
      }
    })
  }


  def printTweets(statuses: DStream[String]) = {
    statuses.print()
  }

  def setupTwitter(): Unit = {
    import scala.io.Source

    for (line <- Source.fromFile("./twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

}
