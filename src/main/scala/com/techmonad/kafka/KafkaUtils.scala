package com.techmonad.kafka

import org.apache.spark.sql.{Dataset, SparkSession}

object KafkaUtils1 {

  def getStream(spark: SparkSession, hosts: String, topic: String): Dataset[(String, String)] = {
    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", hosts)
      .option("subscribe", topic)
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
  }

}
