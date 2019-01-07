package com.dyingbleed.corgi.spark.core

import com.dyingbleed.corgi.spark.annotation.EnableMeasure
import com.dyingbleed.corgi.spark.ds.el.{MySQLCompleteEL, MySQLIncrementalEL, OracleCompleteEL, OracleIncrementalEL}
import com.dyingbleed.corgi.spark.ds.{DataSource, DataSourceEL}
import com.dyingbleed.corgi.spark.measure.MeasureInterceptor
import com.google.inject.AbstractModule
import com.google.inject.matcher.Matchers
import com.google.inject.name.Names
import org.apache.spark.sql.SparkSession
import org.joda.time.{LocalDate, LocalDateTime}

/**
  * Created by 李震 on 2018/1/9.
  */
private[spark] class Bootstrap(args: Array[String]) {

  // 加载应用配置
  val conf: Conf = Conf(args)

  def bootstrap(execute: AbstractModule => Unit): Unit = {

    // 配置系统属性
    System.setProperty("hive.metastore.uris", conf.hiveMetastoreUris)

    // 初识化 SparkSession
    val spark = if (Bootstrap.debug) {
      val spark = SparkSession.builder()
        .master("local")
        .appName(conf.appName)
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
        .appName(conf.appName)
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", true) // 支持 Hive 动态分区
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.parquet.writeLegacyFormat", true)
        .getOrCreate()

      // 日志级别
      spark.sparkContext.setLogLevel("WARN")

      spark
    }

    // 依赖注入绑定
    val module = new AbstractModule() {

      override def configure(): Unit = {
        bind(classOf[SparkSession]).toInstance(spark)
        bind(classOf[String]).annotatedWith(Names.named("appName")).toInstance(conf.appName)
        bind(classOf[String]).annotatedWith(Names.named("apiServer")).toInstance(conf.apiServer)

        // 执行日期时间
        val executeDateTime = if (conf.executeTime.isDefined) {
          LocalDate.now().toLocalDateTime(conf.executeTime.get)
        } else {
          LocalDateTime.now()
        }
        bind(classOf[LocalDateTime]).annotatedWith(Names.named("executeDateTime")).toInstance(executeDateTime)

        bind(classOf[Conf]).toInstance(conf) // 配置

        bind(classOf[DataSource])
        val elImplClass = if (conf.sourceDbUrl.startsWith("jdbc:mysql")) {
          conf.mode match {
            case ODSMode.COMPLETE => classOf[MySQLCompleteEL]
            case ODSMode.UPDATE | ODSMode.APPEND => classOf[MySQLIncrementalEL]
          }
        } else if (conf.sourceDbUrl.startsWith("jdbc:oracle:thin")) {
          conf.mode match {
            case ODSMode.COMPLETE => classOf[OracleCompleteEL]
            case ODSMode.UPDATE | ODSMode.APPEND => classOf[OracleIncrementalEL]
          }
        } else {
          throw new RuntimeException("不支持的数据源")
        }
        bind(classOf[DataSourceEL]).to(elImplClass)

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

}

object Bootstrap {

  def apply(args: Array[String]): Bootstrap = new Bootstrap(args)

  def debug: Boolean = {
    val debug = System.getenv("CORGI_DEBUG")
    debug != null && debug.equals("true")
  }

}
