package com.dyingbleed.corgi.spark.core

import com.dyingbleed.corgi.spark.annotation.{Complete, EnableMeasure, Incremental}
import com.dyingbleed.corgi.spark.bean.Table
import com.dyingbleed.corgi.spark.core.DBMSVendor._
import com.dyingbleed.corgi.spark.ds.el._
import com.dyingbleed.corgi.spark.ds.el.split._
import com.dyingbleed.corgi.spark.ds.{DataSource, EL}
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
  private[this] val conf: Conf = Conf(args)

  // 表元数据
  private[this] val tableMeta: Table = Table(
    conf.sourceDb,
    conf.sourceTable,
    conf.sourceDbUrl,
    conf.sourceDbUser,
    conf.sourceDbPassword,
    Option(conf.sourceTimeColumn)
  )

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
        bind(classOf[LocalDateTime]).toInstance(executeDateTime)

        bind(classOf[Conf]).toInstance(conf) // 配置

        bind(classOf[Table]).toInstance(tableMeta) // 表元数据

        bind(classOf[EL]) // Extract-Load

        // 全量数据
        val completeDataSourceImplClass = tableMeta.vendor match {
          case MYSQL => classOf[MySQLCompleteDataSource]
          case ORACLE => classOf[OracleCompleteDataSource]
        }
        bind(classOf[CompleteDataSource]).to(completeDataSourceImplClass)

        // 全量分片数据
        val completeSplitDataSourceImplClass = tableMeta.vendor match {
          case MYSQL => classOf[MySQLCompleteSplitDataSource]
          case ORACLE => classOf[OracleCompleteSplitDataSource]
        }
        bind(classOf[DataSource]).annotatedWith(classOf[Complete]).to(completeSplitDataSourceImplClass)

        // 增量数据
        val incrementalDataSourceImplClass = tableMeta.vendor match {
          case MYSQL => classOf[MySQLIncrementalDataSource]
          case ORACLE => classOf[OracleIncrementalDataSource]
        }
        bind(classOf[IncrementalDataSource]).to(incrementalDataSourceImplClass)

        // 增量分片数据
        val incrementalSplitDataSourceImplClass = tableMeta.vendor match {
          case MYSQL => classOf[MySQLIncrementalSplitDataSource]
          case ORACLE => classOf[OracleIncrementalSplitDataSource]
        }
        bind(classOf[DataSource]).annotatedWith(classOf[Incremental]).to(incrementalSplitDataSourceImplClass)

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

  /**
    * 是否 DEBUG 模式
    * */
  def debug: Boolean = {
    val debug = System.getenv("CORGI_DEBUG")
    debug != null && debug.equals("true")
  }

}