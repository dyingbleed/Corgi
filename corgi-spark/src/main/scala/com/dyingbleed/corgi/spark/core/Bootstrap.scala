package com.dyingbleed.corgi.spark.core

import java.util.Properties

import com.dyingbleed.corgi.spark.measure.{EnableMeasure, MeasureInterceptor}
import com.google.inject.AbstractModule
import com.google.inject.name.Names
import com.google.common.base.Preconditions._
import com.google.inject.matcher.Matchers
import org.apache.spark.sql.SparkSession

/**
  * Created by 李震 on 2018/1/9.
  */
case class AppConf(hiveMetastoreUri: String, apiServer: String)

private[spark] class Bootstrap(args: Array[String]) {

  val appName = args(0)

  def bootstrap(execute: (AbstractModule) => Unit): Unit = {

    // 加载应用配置
    val appConf = loadAppConf()

    // 配置系统属性
    System.setProperty("hive.metastore.uris", appConf.hiveMetastoreUri)

    // 初识化 SparkSession
    val spark = if (appName.startsWith("test") || appName.endsWith("test")) {
      val spark = SparkSession.builder()
        .master("local")
        .appName(appName)
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", true.toString) // 支持 Hive 动态分区
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
        .config("hive.exec.dynamic.partition", true.toString) // 支持 Hive 动态分区
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate()

      // 日志级别
      spark.sparkContext.setLogLevel("WARN")

      spark
    }

    // 依赖注入绑定
    val module = new AbstractModule() {

      override def configure(): Unit = {
        bind(classOf[SparkSession]).toInstance(spark)
        bind(classOf[String]).annotatedWith(Names.named("appName")).toInstance(appName)
        bind(classOf[String]).annotatedWith(Names.named("apiServer")).toInstance(appConf.apiServer) // API 服务地址
        bind(classOf[Conf]) // 配置

        val measureInterceptor = new MeasureInterceptor
        requestInjection(measureInterceptor)
        bindInterceptor(Matchers.any, Matchers.annotatedWith(classOf[EnableMeasure]), measureInterceptor) // 绑定注解
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

  private def loadAppConf(): AppConf = {
    val properties = new Properties()
    val propertiesIn = classOf[Bootstrap].getClassLoader.getResourceAsStream("spark.properties")
    properties.load(propertiesIn)
    propertiesIn.close()

    val hiveMetastoreUri = properties.getProperty("hive.metastore.uris")
    checkNotNull(hiveMetastoreUri)
    val apiServer = properties.getProperty("api.server")
    checkNotNull(apiServer)

    AppConf(hiveMetastoreUri, apiServer)
  }

}

object Bootstrap {

  def apply(args: Array[String]): Bootstrap = new Bootstrap(args)

}
