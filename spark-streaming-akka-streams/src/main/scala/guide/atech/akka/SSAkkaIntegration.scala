package guide.atech.akka

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import guide.atech.schema.{Car, CarSchema}
import org.apache.spark.sql.{Dataset, SparkSession}

object SSAkkaIntegration {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  private def writeCarsToAkka(): Unit = {

    val carsDS = spark
      .readStream
      .schema(CarSchema.schema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch { (batch: Dataset[Car], batchID: Long) =>

        batch.foreachPartition { cars: Iterator[Car] =>

          // this code is run by a single executor
          // Remember ActorSystem are NOT serializable
          val system = ActorSystem(s"SourceSystem$batchID", ConfigFactory.load("akkaconfig/remoteActors.conf"))
          val entryPoint = system.actorSelection("akka://ReceiverSystem@localhost:2552/user/entrypoint")

          // send all the data
          cars.foreach( car => entryPoint ! car)

        }
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeCarsToAkka()
  }

}

object ReceiverSystem {

  implicit val actorSystem = ActorSystem("ReceiverSystem", ConfigFactory.load("akkaconfig/remoteActors.conf").getConfig("remoteSystem"))
  implicit val actorMatetializer = ActorMaterializer()

  class Destination extends Actor with ActorLogging {
    override def receive = {
      case m => log.info(m.toString)
        //println(m)
    }
  }

  object EntryPoint {
    def props(destination: ActorRef) = Props(new EntryPoint(destination))
  }

  class EntryPoint(destination: ActorRef) extends Actor with ActorLogging {
    override def receive = {
      case m => log.info(s"Received $m")
        //println(m)
        destination ! m
    }
  }

//  def main(args: Array[String]): Unit = {
//
//    val destination = actorSystem.actorOf(Props[Destination], "destination")
//    val entryPoint = actorSystem.actorOf(EntryPoint.props(destination), "entrypoint")
//
//  }

  // Akka Streams
  def main(args: Array[String]): Unit = {

    val source = Source.actorRef[Car](bufferSize = 10, overflowStrategy = OverflowStrategy.dropHead)
    val sink = Sink.foreach[Car](println)
    val runnableGraph = source.to(sink)
    val destination: ActorRef = runnableGraph.run()


    val entryPoint = actorSystem.actorOf(EntryPoint.props(destination), "entrypoint")

  }
}
