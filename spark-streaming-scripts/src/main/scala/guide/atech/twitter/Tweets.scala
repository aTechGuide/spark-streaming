package guide.atech.twitter

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Tweets extends App {

    // Uses org.apache.bahir

    TweetUtils.setupTwitter()

    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(status => status.getText)

    ssc.sparkContext.setLogLevel("ERROR")

    TweetUtils.mostPopularHashtag(statuses)

    // Kick it all off
    ssc.checkpoint("~/code/checkpoint")
    ssc.start()
    ssc.awaitTermination()
}
