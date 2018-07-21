package com.techmonad

import com.techmonad.kafka.KafkaUtils1
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.control.NonFatal

//TODO: Structured Streaming does not have support for ignite db
object StreamingApp /*extends App*/ {

  val spark: SparkSession = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local[*]")
    .getOrCreate()
  val stream: Dataset[(String, String)] = KafkaUtils1.getStream(spark, "localhost:9092", "data_queue")

  spark.sparkContext.setLogLevel("ERROR")


  import spark.implicits._

  val sensorDataStream: Dataset[SensorData] = stream
    .map { case (_, value) =>
      val arr = value.split(",")
      SensorData(arr(0).toInt, arr(1).toDouble, arr(2).toLong)
    }
  val query: StreamingQuery = sensorDataStream.writeStream
    .queryName("counts")
    .outputMode("append")
    .format("memory")
    .start()


  sensorDataStream.write
    .format(FORMAT_IGNITE)
    .option(OPTION_CONFIG_FILE, CONFIG)
    .option(OPTION_TABLE, "sensor")
    .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
    .option(OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
    .mode(SaveMode.Append)
    .save()
  private val CONFIG = "/home/satendra/open-source/rest-full-streaming/config/example-ignite.xml"

  /*
    sensorDataStream
      .writeStream
      .foreach{data => data.w}
      .start()
  */

  /**
    * Query data on separate thread
    */
  Future {
    while (true) {
      try {
        Thread.sleep(100)
        val query = "select * from counts"
        println("-----------------------------------------------------")
        val result = spark.sql(query).collect().toList
        println("Result " + result)
        println("total result = " + result.length)
      } catch {
        case NonFatal(_) =>
          println("hug.......")
      }
    }
  }

  query.awaitTermination()

}

case class SensorData(id: Long, temperature: Double, time: Long)
