package com.dyingbleed.corgi.spark.ds.el

import com.dyingbleed.corgi.spark.bean.Table
import com.dyingbleed.corgi.spark.ds.DataSourceEL
import com.dyingbleed.corgi.spark.ds.el.split.SplitManager
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDateTime

/**
  * Created by 李震 on 2018/6/26.
  */
private[spark] abstract class CompleteEL extends DataSourceEL {

  /**
    * 加载数据源
    *
    * @return
    **/
  override def loadSourceDF: DataFrame = {
    val cardinlity = tableMeta.cardinality()
    val splitManager = SplitManager(spark, tableMeta, LocalDateTime.now())

    if (cardinlity > 100000 && splitManager.canSplit) {
      /*
       * 大于 100000 条数据，分区
       * */
      splitManager.loadDF
    } else {
      /*
       * 小于 100000 条数据，不进行分区
       * */
      jdbcDF(completeSQL(tableMeta))
    }
  }

  /**
    * 全量数据 SQL
    * */
  protected def completeSQL(tableMeta: Table): String

}
