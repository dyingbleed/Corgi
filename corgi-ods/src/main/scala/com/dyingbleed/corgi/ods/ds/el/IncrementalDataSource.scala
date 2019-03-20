package com.dyingbleed.corgi.ods.ds.el

import com.dyingbleed.corgi.ods.ds.DataSource
import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2018/6/26.
  */
private[ods] abstract class IncrementalDataSource extends DataSource {

  /**
    * 加载数据源
    *
    * @return
    **/
  override def loadSourceDF: DataFrame = {
    jdbcDF(incrementalSQL)
  }

  /**
    * 增量数据 SQL
    **/
  protected def incrementalSQL: String

}
