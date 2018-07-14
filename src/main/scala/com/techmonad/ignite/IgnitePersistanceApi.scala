package com.techmonad.ignite

import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.{DataFrame, SaveMode}

object IgnitePersistanceApi {

  private val CONFIG = "/home/satendra/open-source/rest-full-streaming/config/example-ignite.xml"

  def save(df: DataFrame)={
    df.write
      .format(FORMAT_IGNITE)
      .option(OPTION_CONFIG_FILE, CONFIG)
      .option(OPTION_TABLE, "sensor")
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
      .option(OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
      .mode(SaveMode.Append)
      .save()
  }

}
