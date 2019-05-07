package com.dyingbleed.corgi.client

import com.dyingbleed.corgi.client.dm.{DM, DMImpl}
import com.dyingbleed.corgi.client.ods.{ODS, ODSImpl}
import org.apache.spark.sql.SparkSession

/**
  * Created by 李震 on 2019-05-05.
  */
object Corgi {

  def builder(): CorgiBuilder = {
    new CorgiBuilder
  }

  class CorgiBuilder private[Corgi] () {

    private[this] var url: String = _

    private[this] var spark: SparkSession = _

    def withURL(url: String): CorgiBuilder = {
      this.url = url
      this
    }

    def withSparkSession(spark: SparkSession): CorgiBuilder = {
      this.spark = spark
      this
    }

    def build(): Corgi = {
      assert(this.url != null)
      assert(this.spark != null)

      new Corgi(url, spark)
    }

  }

}

class Corgi private[Corgi] (url: String, spark: SparkSession) {

  def ods: ODS = {
    new ODSImpl(url, spark)
  }

  def dm: DM = {
    new DMImpl(url, spark)
  }

}

