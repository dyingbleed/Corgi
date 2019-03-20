package com.dyingbleed.corgi.ods

import com.dyingbleed.corgi.core.constant.DBMSVendor.{MYSQL, ORACLE}
import com.dyingbleed.corgi.ods.annotation.{Complete, EnableMeasure, Incremental}
import com.dyingbleed.corgi.ods.bean.Table
import com.dyingbleed.corgi.ods.core.Conf
import com.dyingbleed.corgi.ods.ds.{DataSource, EL}
import com.dyingbleed.corgi.ods.ds.el._
import com.dyingbleed.corgi.ods.ds.el.split.{MySQLCompleteSplitDataSource, MySQLIncrementalSplitDataSource, OracleCompleteSplitDataSource, OracleIncrementalSplitDataSource}
import com.dyingbleed.corgi.ods.measure.MeasureInterceptor
import com.google.inject.AbstractModule
import com.google.inject.matcher.Matchers
import com.google.inject.name.Names
import org.apache.spark.sql.SparkSession
import org.joda.time.{LocalDateTime, LocalTime}

/**
  * Created by 李震 on 2019/3/20.
  */
class ApplicationModule private[ApplicationModule] (spark: SparkSession, args: Array[String]) extends AbstractModule {

  private[this] var conf: Conf = _

  private[this] var tableMeta: Table = _

  private[ApplicationModule] def init(): Unit = {
    conf = Conf(args)
    tableMeta = Table(
      conf.sourceDb,
      conf.sourceTable,
      conf.sourceDbUrl,
      conf.sourceDbUser,
      conf.sourceDbPassword,
      Option(conf.sourceTimeColumn)
    )
  }

  override def configure(): Unit = {
    bind(classOf[SparkSession]).toInstance(spark)
    bind(classOf[String]).annotatedWith(Names.named("appName")).toInstance(conf.appName)
    bind(classOf[String]).annotatedWith(Names.named("apiServer")).toInstance(conf.apiServer)

    // 执行日期时间
    val executeDateTime = if (conf.executeTime.isDefined) {
      conf.executeDate.toLocalDateTime(conf.executeTime.get)
    } else {
      conf.executeDate.toLocalDateTime(LocalTime.now())
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

object ApplicationModule {

  def apply(spark: SparkSession, args: Array[String]): ApplicationModule = new ApplicationModule(spark, args)
  
}