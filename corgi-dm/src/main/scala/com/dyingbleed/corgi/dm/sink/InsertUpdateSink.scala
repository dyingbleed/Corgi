package com.dyingbleed.corgi.dm.sink
import com.dyingbleed.corgi.dm.core.{Conf, SinkTable}
import com.google.inject.Inject
import org.apache.spark.sql.SparkSession

/**
  * Created by 李震 on 2019/3/12.
  */
abstract class InsertUpdateSink extends Sink {

  @Inject
  var conf: Conf = _

  @Inject
  var sinkTable: SinkTable = _

  @Inject
  var spark: SparkSession = _

}
