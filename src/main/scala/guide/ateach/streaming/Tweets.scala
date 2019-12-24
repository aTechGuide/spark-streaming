package guide.ateach.streaming

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import guide.ateach.utils.TweetUtils

object Tweets extends App {

    TweetUtils.setupTwitter()

    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(status => status.getText())

    ssc.sparkContext.setLogLevel("ERROR")

    TweetUtils.mostPopularHashtag(statuses)

    // Kick it all off
    ssc.checkpoint("/Users/kamali/mcode/tmp/")
    ssc.start()
    ssc.awaitTermination()
}
