package com.dyingbleed.corgi.dm.sink
import com.dyingbleed.corgi.dm.annotation.EnableSinkOptimization
import com.dyingbleed.corgi.dm.core.Conf
import com.google.inject.Inject
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created by 李震 on 2019/3/12.
  */
class InsertSink extends Sink {

  @Inject
  var conf: Conf = _

  @EnableSinkOptimization
  override def sink(df: DataFrame): Unit = {
    df.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", conf.url)
      .option("driver", conf.sinkVendor.getDriverClassName)
      .option("dbtable", s"${conf.sinkDB}.${conf.sinkTable}")
      .option("user", conf.username)
      .option("password", conf.password)
      .save()
  }

}
