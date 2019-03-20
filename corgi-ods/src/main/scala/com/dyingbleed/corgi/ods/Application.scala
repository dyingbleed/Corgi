package com.dyingbleed.corgi.ods

import com.dyingbleed.corgi.ods.annotation.EnableMeasure
import com.dyingbleed.corgi.ods.ds.EL
import com.google.inject._
import org.apache.spark.sql.SparkSession

/**
  * Created by 李震 on 2018/3/1.
  */
object Application {

  lazy val debug: Boolean = "true".equals(System.getenv("CORGI_DEBUG"))

  def main(args: Array[String]): Unit = {
    val spark = if (debug) {
      val spark = SparkSession.builder()
        .master("local")
        .appName("corgi-ods")
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", true) // 支持 Hive 动态分区
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.parquet.writeLegacyFormat", true)
        .getOrCreate()

      // 日志级别
      spark.sparkContext.setLogLevel("DEBUG")

      spark
    } else {
      val spark = SparkSession.builder()
        .master("yarn")
        .appName("corgi-ods")
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", true) // 支持 Hive 动态分区
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.parquet.writeLegacyFormat", true)
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
  var el: EL = _

  @EnableMeasure
  def run(): Unit = {
    // 加载数据
    val df = el.loadSourceDF

    // 保存数据
    if (Application.debug) {
      df.explain()
    } else {
      el.persistSinkDF(df)
    }
  }

}