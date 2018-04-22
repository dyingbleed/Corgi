package com.dyingbleed.corgi.spark

import com.google.inject.name.Named
import com.google.inject.{Guice, Inject}
import com.dyingbleed.corgi.spark.core.{Bootstrap, DataSource}
import org.apache.spark.sql.SparkSession

/**
  * Created by 李震 on 2018/3/1.
  */
object Application {

  def main(args: Array[String]): Unit = {
    Bootstrap(args).bootstrap(execute)
  }

  def execute(spark: SparkSession, appName: String): Unit = {
    val injector = Guice.createInjector(new ApplicationModule(spark, appName))
    injector.getInstance(classOf[Application]).run()
  }

}

class Application @Inject() (spark: SparkSession, @Named("appName") appName: String) {

  @Inject
  var dataSource: DataSource = _

  def run(): Unit = {
    // 加载数据
    val sourceDF = dataSource.loadSourceDF

    // 保存数据
    dataSource.persistSinkDF(sourceDF)
  }

}
