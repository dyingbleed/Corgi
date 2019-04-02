package com.dyingbleed.corgi.dm

import com.dyingbleed.corgi.core.constant.{DBMSVendor, Mode}
import com.dyingbleed.corgi.core.exception.NotSupportDBMSException
import com.dyingbleed.corgi.dm.annotation.EnableSinkOptimization
import com.dyingbleed.corgi.dm.aop.SinkOptimizationAOP
import com.dyingbleed.corgi.dm.core.Conf
import com.dyingbleed.corgi.dm.sink._
import com.dyingbleed.corgi.dm.source.Source
import com.google.inject.AbstractModule
import com.google.inject.matcher.Matchers
import com.google.inject.name.Names
import org.apache.spark.sql.SparkSession

/**
  * Created by 李震 on 2019/3/12.
  */
class ApplicationModule private[ApplicationModule] (spark: SparkSession, args: Array[String]) extends AbstractModule {

  lazy val debug: Boolean = "true".equals(System.getenv("CORGI_DEBUG"))

  private[this] var conf: Conf = _

  private[ApplicationModule] def init(): Unit = {
    conf = Conf(args)
  }

  override def configure(): Unit = {
    bind(classOf[SparkSession]).toInstance(spark)
    bind(classOf[Conf]).toInstance(conf)
    bind(classOf[String]).annotatedWith(Names.named("appName")).toInstance(conf.appName)

    // SinkTable
    val sinkTableClass = conf.sinkVendor match {
      case DBMSVendor.MYSQL => {
        Class.forName("com.dyingbleed.corgi.dm.sink.MySQLSinkTable")
          .asSubclass(classOf[SinkTable])
      }
      case DBMSVendor.ORACLE => {
        Class.forName("com.dyingbleed.corgi.dm.sink.OracleSinkTable")
          .asSubclass(classOf[SinkTable])
      }
      case _ => throw new NotSupportDBMSException
    }
    bind(classOf[SinkTable]).to(sinkTableClass)

    // Source
    bind(classOf[Source])

    // Sink
    val sinkClass = conf.mode match {
      case Mode.APPEND => classOf[InsertSink]
      case Mode.COMPLETE => classOf[InsertOverwriteSink]
      case Mode.UPDATE => {
        conf.sinkVendor match {
          case DBMSVendor.MYSQL => {
            Class.forName("com.dyingbleed.corgi.dm.sink.MySQLInsertUpdateSink")
              .asSubclass(classOf[Sink])
          }
          case DBMSVendor.ORACLE => {
            Class.forName("com.dyingbleed.corgi.dm.sink.OracleInsertUpdateSink")
              .asSubclass(classOf[Sink])
          }
          case _ => throw new NotSupportDBMSException
        }
      }
    }
    bind(classOf[Sink]).to(sinkClass)

    // AOP
    val sinkOptimizationAOP = new SinkOptimizationAOP
    requestInjection(sinkOptimizationAOP)
    bindInterceptor(Matchers.subclassesOf(classOf[Sink]), Matchers.annotatedWith(classOf[EnableSinkOptimization]), sinkOptimizationAOP)
  }

}

object ApplicationModule {

  def apply(spark: SparkSession, args: Array[String]): ApplicationModule = {
    val module = new ApplicationModule(spark, args)
    module.init()
    module
  }

}
