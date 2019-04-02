package com.dyingbleed.corgi.dm

import com.dyingbleed.corgi.dm.core.Conf
import com.dyingbleed.corgi.dm.sink.Sink
import com.dyingbleed.corgi.dm.source.Source
import com.google.inject.{Guice, Inject}
import org.apache.spark.sql.SparkSession

/**
  * Created by 李震 on 2019/3/12.
  */
object Application {

  def main(args: Array[String]): Unit = {
    val spark = if (Conf.debug) {
      val spark = SparkSession.builder()
        .master("local")
        .appName("corgi-ods")
        .enableHiveSupport()
        .getOrCreate()

      // 日志级别
      spark.sparkContext.setLogLevel("DEBUG")

      spark
    } else {
      val spark = SparkSession.builder()
        .master("yarn")
        .appName("corgi-ods")
        .enableHiveSupport()
        .getOrCreate()

      // 日志级别
      spark.sparkContext.setLogLevel("WARN")

      spark
    }

    execute(spark, args)
  }

  def execute(spark: SparkSession, args: Array[String]): Unit = {
    val injector = Guice.createInjector(ApplicationModule(spark, args))
    injector.getInstance(classOf[Application]).run()
  }

}

class Application {

  @Inject
  var source: Source = _

  @Inject
  var sink: Sink = _

  def run(): Unit = {
    val sourceDF = source.source()
    sink.sink(sourceDF)
  }

}