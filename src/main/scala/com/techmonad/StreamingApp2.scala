package com.techmonad


import com.techmonad.ignite.{IgnitePersistenceApi, IgniteUtils}
import com.techmonad.kafka.KafkaUtility
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}


object StreamingApp2 extends App {

  val ignite = IgniteUtils.setupServerAndData

  val spark: SparkSession = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")


  import spark.implicits._

  val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Milliseconds(100))

  val messages: InputDStream[ConsumerRecord[String, String]] = KafkaUtility.createDStreamFromKafka(ssc, List("data_queue3"))

  val sensorData: DStream[SensorData] = messages.map { record =>
    val arr = record.value().split(",")
    //println("receiving time  " + arr(2).toLong)
    SensorData(arr(0).toLong, arr(1).toDouble, arr(2).toLong)
  }
  sensorData.persist()
  sensorData.foreachRDD { rdd =>
    if(!rdd.isEmpty ) IgnitePersistenceApi.save(rdd.toDF)
  }

  ssc.start()
  ssc.awaitTermination()
}
