package com.dyingbleed.corgi.ods.ds.el

import com.dyingbleed.corgi.ods.core.Constants
import com.dyingbleed.corgi.core.constant.Mode._

/**
  * Created by 李震 on 2018/12/28.
  */
private[ods] class MySQLCompleteDataSource extends CompleteDataSource {

  override protected def completeSQL: String = {
    conf.mode match {
      case COMPLETE => {
        s"""
           |(SELECT
           |  ${tableMeta.toSelectExpr(tableMeta.columns)}
           |FROM ${tableMeta.db}.${tableMeta.table}
           |) t
        """.stripMargin
      }
      case UPDATE | APPEND => {
        val normalColumns = tableMeta.columns
          .filter(c => c.getName.equals(tableMeta.tsColumnName.get))

        s"""
           |(SELECT
           |  ${tableMeta.toSelectExpr(normalColumns)},
           |  IFNULL(${tableMeta.tsColumnName.get}, TIMESTAMP('${tableMeta.tsDefaultVal.toString(Constants.DATETIME_FORMAT)}')) AS ${tableMeta.tsColumnName.get}
           |FROM ${tableMeta.db}.${tableMeta.table}
           |WHERE ${tableMeta.tsColumnName.get} < TIMESTAMP('${executeDateTime.toString(Constants.DATETIME_FORMAT)}')
           |) t
        """.stripMargin
      }
    }
  }

}
