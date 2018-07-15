package com.techmonad.ignite

import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.CacheConfiguration
import java.lang.{Long => JLong, String => JString, Double => JDouble }

object IgniteUtils {

   val CONFIG = "config/example-ignite.xml"
   val CACHE_NAME = "testCache"

  def setupServerAndData: Ignite = {
    //Starting Ignite.
    Ignition.setClientMode(true)
    val ignite: Ignite = Ignition.start(CONFIG)

    //Creating first test cache.
    val ccfg = new CacheConfiguration[JLong, JString](CACHE_NAME).setSqlSchema("PUBLIC")

    val cache = ignite.getOrCreateCache(ccfg)

    //Creating SQL table.
    try {
      cache.query(new SqlFieldsQuery(
        "CREATE TABLE sensor (id LONG, temperature DOUBLE, time Long, PRIMARY KEY (id)) " +
          "WITH \"backups=1\"")).getAll
    }
    try{
       println("creating index....................")
      cache.query(new SqlFieldsQuery(
        "CREATE INDEX IF NOT EXISTS time_idx ON sensor (time);")).getAll
    }

    //Inserting some data to tables.
 /*   val qry = new SqlFieldsQuery("INSERT INTO sensor (id, temperature, time) values (?, ?, ?)")
    try {
      cache.query(qry.setArgs(1L.asInstanceOf[JLong], 102.0.asInstanceOf[JDouble], System.currentTimeMillis().asInstanceOf[JLong])).getAll
      cache.query(qry.setArgs(2L.asInstanceOf[JLong], 92.0.asInstanceOf[JDouble], System.currentTimeMillis().asInstanceOf[JLong])).getAll
      cache.query(qry.setArgs(3L.asInstanceOf[JLong], 82.0.asInstanceOf[JDouble], System.currentTimeMillis().asInstanceOf[JLong])).getAll
      cache.query(qry.setArgs(4L.asInstanceOf[JLong], 98.0.asInstanceOf[JDouble], System.currentTimeMillis().asInstanceOf[JLong])).getAll
    }*/
    ignite
  }


}
