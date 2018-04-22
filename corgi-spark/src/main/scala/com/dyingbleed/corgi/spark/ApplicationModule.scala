package com.dyingbleed.corgi.spark

import com.google.inject.AbstractModule
import com.google.inject.name.Names
import com.dyingbleed.corgi.spark.core.{Conf, DataSource, Metadata}
import org.apache.spark.sql.SparkSession

/**
  * Created by 李震 on 2018/3/1.
  */
class ApplicationModule(spark: SparkSession, appName: String) extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[SparkSession]).toInstance(spark)
    bind(classOf[String]).annotatedWith(Names.named("appName")).toInstance(appName)
    bind(classOf[Conf])
    bind(classOf[Metadata])
    bind(classOf[DataSource])
  }

}
