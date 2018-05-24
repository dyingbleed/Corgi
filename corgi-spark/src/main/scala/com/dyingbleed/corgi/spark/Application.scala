package com.dyingbleed.corgi.spark

import com.dyingbleed.corgi.spark.core.{Bootstrap, DataSource}
import com.google.inject.{AbstractModule, Guice, Inject}

/**
  * Created by 李震 on 2018/3/1.
  */
object Application {

  def main(args: Array[String]): Unit = {
    Bootstrap(args).bootstrap(execute)
  }

  def execute(module: AbstractModule): Unit = {
    val injector = Guice.createInjector(module)
    injector.getInstance(classOf[Application]).run()
  }

}

class Application {

  @Inject
  var dataSource: DataSource = _

  def run(): Unit = {
    // 加载数据
    val sourceDF = dataSource.loadSourceDF

    // 保存数据
    dataSource.persistSinkDF(sourceDF)
  }

}
