package com.dyingbleed.corgi.spark

import com.dyingbleed.corgi.spark.annotation.{EnableMeasure, MySQLIncrementalSource, OracleIncrementalSource}
import com.dyingbleed.corgi.spark.core.Bootstrap
import com.dyingbleed.corgi.spark.ds.el.{CompleteEL, MySQLIncrementalEL, OracleIncrementalEL}
import com.dyingbleed.corgi.spark.ds.{DataSource, DataSourceEL}
import com.google.inject._
import com.google.inject.name.Names

/**
  * Created by 李震 on 2018/3/1.
  */
object Application {

  def main(args: Array[String]): Unit = {
    Bootstrap(args).bootstrap(execute)
  }

  def execute(module: AbstractModule): Unit = {
    val injector = Guice.createInjector(module, new ApplicationModule)
    injector.getInstance(classOf[Application]).run()
  }

}

class Application {

  @Inject
  var dataSource: DataSource = _

  @EnableMeasure
  def run(): Unit = {
    // 加载数据
    val sourceDF = dataSource.loadSourceDF

    // 保存数据
    if (Bootstrap.debug) {
      sourceDF.explain()
    } else {
      dataSource.persistSinkDF(sourceDF)
    }
  }

}

private class ApplicationModule extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[DataSource])
    bind(classOf[DataSourceEL])
      .annotatedWith(Names.named("COMPLETE"))
      .to(classOf[CompleteEL])
    bind(classOf[DataSourceEL])
      .annotatedWith(classOf[MySQLIncrementalSource])
      .to(classOf[MySQLIncrementalEL])
    bind(classOf[DataSourceEL])
      .annotatedWith(classOf[OracleIncrementalSource])
      .to(classOf[OracleIncrementalEL])
  }

}
