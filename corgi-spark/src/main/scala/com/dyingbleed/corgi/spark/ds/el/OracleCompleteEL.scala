package com.dyingbleed.corgi.spark.ds.el

import com.dyingbleed.corgi.spark.bean.Table

/**
  * Created by 李震 on 2018/12/28.
  */
private[spark] class OracleCompleteEL extends CompleteEL {

  override protected def completeSQL(tableMeta: Table): String = {
    s"""
      |(SELECT
      |  *
      |FROM ${tableMeta.db}.${tableMeta.table}
      |) t
    """.stripMargin
  }

}
