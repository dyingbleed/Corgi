package com.dyingbleed.corgi.spark.core

import org.apache.spark.sql.SparkSession

/**
  * Created by 李震 on 2018/1/9.
  */
class Bootstrap(args: Array[String]) {

  val appName = args(0)

  def bootstrap(execute: (SparkSession, String) => Unit): Unit = {

    val spark = if (appName.startsWith("test") || appName.endsWith("test")) {
      val spark = SparkSession.builder()
        .master("local[2]")
        .appName(appName)
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", "true") // 支持 Hive 动态分区
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate()

      // 日志级别
      spark.sparkContext.setLogLevel("DEBUG")

      spark
    } else {
      val spark = SparkSession.builder()
        .master("yarn")
        .appName(appName)
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", true) // 支持 Hive 动态分区
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate()

      // 日志级别
      spark.sparkContext.setLogLevel("WARN")

      spark
    }

    try {
      execute(spark, appName) // 执行
    } catch {
      case e: Exception => throw new RuntimeException(e)
    } finally {
      spark.close()
    }

  }

}

object Bootstrap {

  def apply(args: Array[String]): Bootstrap = new Bootstrap(args)

}
