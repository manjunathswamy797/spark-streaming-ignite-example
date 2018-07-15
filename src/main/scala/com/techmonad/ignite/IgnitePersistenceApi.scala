package com.techmonad.ignite

import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.{DataFrame, SaveMode}

object IgnitePersistenceApi {

  def save(df: DataFrame): Unit = {
    println("Saving to ignite.......")
    df.write
      .format(FORMAT_IGNITE)
      .option(OPTION_CONFIG_FILE, IgniteUtils.CONFIG)
      .option(OPTION_TABLE, "sensor")
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
      .option(OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
      .mode(SaveMode.Append)
      .save()
  }

}
