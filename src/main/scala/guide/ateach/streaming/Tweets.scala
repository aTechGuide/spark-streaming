package guide.ateach.streaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import guide.ateach.utils.TweetUtils

object Tweets extends App {

    // Configure Twitter credentials using twitter.txt
    Utilities.setupTwitter()

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    Utilities.setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())

    TweetUtils.printTweets(statuses)
    TweetUtils.saveTweets(statuses)
    TweetUtils.averageTweetLength(statuses)

    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
}
