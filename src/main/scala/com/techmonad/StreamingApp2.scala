package com.techmonad


import com.techmonad.ignite.{IgnitePersistanceApi, IgniteUtils}
import com.techmonad.kafka.KafkaUtility
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object StreamingApp2 extends App {

  val ignite = IgniteUtils.setupServerAndData


  val spark: SparkSession = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  import spark.implicits._

  val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Milliseconds(10))

  val messages: InputDStream[ConsumerRecord[String, String]] = KafkaUtility.createDStreamFromKafka(ssc, List("data_queue"))

  val sensorData = messages.map { record =>
    println("Getting " + record)
    val arr = record.value().split(",")
    SensorData(arr(0).toInt, arr(1).toDouble, arr(2).toLong)
  }

  sensorData.foreachRDD { rdd =>
    println("Saving to .............................")
    IgnitePersistanceApi.save(rdd.toDF)
  }

  Future {
    while (true) {
      try {
        Thread.sleep(1000)
        val cache = ignite.cache[Any, Any](IgniteUtils.CACHE_NAME)
        //Reading saved data from Ignite.
        val data = cache.query(new SqlFieldsQuery("SELECT id,  temperature, time FROM sensor")).getAll
        data.foreach { row ⇒ println(row.mkString("[", ", ", "]")) }
        println("-----------------------------------------------------")
      } catch {
        case e =>
          println("hug.......")
      }
    }
  }

  ssc.start()
  ssc.awaitTermination()
}
