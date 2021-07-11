package pubsub

import java.nio.charset.StandardCharsets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AirlinesSubscriber {
  def parseLine(line: String): (String,String,Int) = {
    // Split by commas
      val field = line.split(",")
      // Extract depdelay,origin,dest and uniquecarrier fields
    var depDelay = 0
      try {
        depDelay = field(15).toInt
      } catch {
        case e: Exception => None
      }
      val origin = field(16)
      //val dest = field(17)
      val uniquecarrier = field(8);

    ( origin,uniquecarrier,depDelay)
  }
  def main(argv: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR)
    val projectID = "starlit-channel-317607"
    val windowLength = 60
    val slidingInterval = 10
    val totalRunningTime = 60
    //val checkpointDirectory = "C:\\checkpoint"
    val checkpointDirectory = argv(0)
    println("Check point directory => "+checkpointDirectory)
    //val ssc = StreamingContext.getOrCreate(checkpointDirectory,
    //  () => createContext(projectID, windowLength, slidingInterval, checkpointDirectory))

    val sparkConf = new SparkConf().setAppName("Airlines Data Analysis").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(slidingInterval.toInt))
    ssc.checkpoint(checkpointDirectory)

    val messagesStream: DStream[String] = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        "airlines-sub",  // Cloud Pub/Sub subscription for incoming tweets
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))

    val depDelayfields = messagesStream.map(parseLine)
    val depDelayFilter = depDelayfields.filter(x => x._3 > 0)
    val depDelayCombine = depDelayFilter.map( x => ((x._1,x._2), if(x._3 > 0) 1 else 0) )
    val depDelayGroup = depDelayCombine.reduceByKeyAndWindow(_+_,Seconds(10))
    val twisted = depDelayGroup.map( x => (x._2,x._1))
    val sorted_ = twisted.transform(rdd =>rdd.sortByKey(false)).map(x=>(x._2,x._1))

    sorted_.foreachRDD((rdd,time) => {
      println(rdd.count())
      rdd.collect().take(10).foreach(println)
      println("next batch ......")
    })


    """
    reduceByAgeAndFriends.foreachRDD((rdd) => {
      val col = rdd.collect()
      col.foreach(println)
      println(java.util.SimpleTimeZone.STANDARD_TIME.toString)
    })
    """
    ssc.start()
    ssc.awaitTermination()
  }
}
