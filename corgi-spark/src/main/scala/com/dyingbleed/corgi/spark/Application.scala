package com.dyingbleed.corgi.spark

import com.dyingbleed.corgi.spark.core.{Bootstrap, Conf}
import com.dyingbleed.corgi.spark.ds.DataSource
import com.dyingbleed.corgi.spark.measure.EnableMeasure
import com.google.inject.{AbstractModule, Binder, Guice, Inject}

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
    dataSource.persistSinkDF(sourceDF)
  }

}

private class ApplicationModule extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[DataSource])
  }

}
