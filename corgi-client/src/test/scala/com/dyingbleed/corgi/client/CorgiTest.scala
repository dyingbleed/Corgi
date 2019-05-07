package com.dyingbleed.corgi.client

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.FunSuite

/**
  * Created by 李震 on 2019-05-07.
  */
class CorgiTest extends FunSuite with SharedSparkContext {

  test("dm run") {
    val spark = new HiveContext(sc = sc).sparkSession

    val corgi = Corgi.builder()
      .withSparkSession(spark)
      .withURL("http://127.0.0.1:18085")
      .build()

    corgi.dm.run("test")
  }

  test("ods data") {
    val hiveContext = new HiveContext(sc = sc)
    val spark = hiveContext.sparkSession

    val corgi = Corgi.builder()
      .withSparkSession(spark)
      .withURL("http://127.0.0.1:18085")
      .build()

    val df = corgi.ods.data("area")

    assert(df != null)
  }

}
