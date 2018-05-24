package com.dyingbleed.corgi.spark.core

import com.google.inject.AbstractModule
import com.google.inject.name.Names
import org.apache.spark.sql.SparkSession

/**
  * Created by 李震 on 2018/1/9.
  */
class Bootstrap(args: Array[String]) {

  val appName = args(0)
  val apiServer = args(1)

  def bootstrap(execute: (AbstractModule) => Unit): Unit = {
    val spark = if (appName.startsWith("test") || appName.endsWith("test")) {
      val spark = SparkSession.builder()
        .master("local")
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
        .master("local")
        .appName(appName)
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", true) // 支持 Hive 动态分区
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate()

      // 日志级别
      spark.sparkContext.setLogLevel("WARN")

      spark
    }

    val module = new AbstractModule() {

      override def configure(): Unit = {
        bind(classOf[SparkSession]).toInstance(spark)
        bind(classOf[String]).annotatedWith(Names.named("appName")).toInstance(appName)
        bind(classOf[String]).annotatedWith(Names.named("apiServer")).toInstance(apiServer)
        bind(classOf[Conf])
        bind(classOf[Metadata])
        bind(classOf[DataSource])
      }

    }

    try {
      execute(module) // 执行
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
