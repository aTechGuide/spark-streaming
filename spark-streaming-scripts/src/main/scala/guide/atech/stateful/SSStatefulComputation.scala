package guide.atech.stateful

import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SSStatefulComputation {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  case class SocialPostRecord(postType: String, count: Int, storageUsed: Int)
  case class SocialPostBulk(postType: String, count: Int, totalStorageUsed: Int)
  case class AveragePostStorage(postType: String, averageStorage: Double)

  import spark.implicits._

  private def readFromSocket: DataFrame = {

    spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
  }

  private def readSocialUpdates: Dataset[SocialPostRecord] = {

    // postType, count, storageGroups
    val data = readFromSocket
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        SocialPostRecord(tokens(0), tokens(1).toInt, tokens(2).toInt)
      }

    data
  }

  private def updateAverageStorage(
                          // Key by which we have made the groups
                          postType: String,
                          // Batch of data associated to the key. Iterator of SocialPostRecord, which is the Type associated with Stream
                          group: Iterator[SocialPostRecord],
                          // Typed with intermediate state that we want to "manually" manage as the group is processed, like an "option".
                          state: GroupState[SocialPostBulk]
                          ): AveragePostStorage // a single value that we will output per the entire group
  = {

    /*
      - Extract the state to start with
      - for all the items in the group
        - aggregate data:
          - Summing up total count
          - summing up the total storage
      - update the state with the new aggregated data
      - return a single value of type AveragePostStorage
     */

    // extracted the state to start with
    val previousBulk =
      if (state.exists) state.get
      else SocialPostBulk(postType, 0, 0)

    // iterate through the group
    val totalAggregatedData = group.foldLeft((0,0)) { (currentData, record) =>
      val (currentCount, currentStorage) = currentData
      (currentCount + record.count, currentStorage + record.storageUsed )
    }

    // update the state with new aggregated data
    val (totaCount, totalStorage) = totalAggregatedData
    val newPostBulk = SocialPostBulk(postType, previousBulk.count + totaCount, previousBulk.totalStorageUsed + totalStorage )

    state.update(newPostBulk)

    // return a single value of type AveragePostStorage
    AveragePostStorage(postType, newPostBulk.totalStorageUsed * 1.0 / newPostBulk.count)

  }

  private def computeAveragePostStorage(): Unit = {
    val socialStream: Dataset[SocialPostRecord] = readSocialUpdates

    // Option 1 [Observation in Stateful Compuration]
    val regularSqlAverageByPostType = socialStream
      .groupByKey(_.postType)
      .agg(sum(col("count")).as("totalCount").as[Int], sum(col("storageUsed")).as("totalStorage").as[Int])
      .selectExpr("key as postType", "totalStorage / totalCount as avgStorage")


    // Option 2
    val averageByPostType = socialStream
      .groupByKey(_.postType)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAverageStorage)
    /*
      GroupStateTimeout.NoTimeout() is like a watermark
      - How late the incoming data could be so that its taken into account
        - So if the data is late, it will not be taken into account in the group that will be passed to update average storage
      - We need to define a watermark for it to work

     */

    averageByPostType
      .writeStream
      .outputMode("update") // append not supported on mapGroupsWithState
      .foreachBatch { (batch: Dataset[AveragePostStorage], _: Long) =>
          batch.show(false)
        }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    /**
      * To produce Input -> nc -lk 12345
      */
    computeAveragePostStorage()
  }

  /*
    Example 1
      text,3,3000
      text,4,5000
      video,1,500000

    Example 2
      text,3,3000
      text,4,5000
      video,1,500000
      audio,3,60000
      text,1,2500
   */
}
