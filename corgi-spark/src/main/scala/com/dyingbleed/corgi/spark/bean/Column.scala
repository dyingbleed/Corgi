package com.dyingbleed.corgi.spark.bean

import java.sql.Types

/**
  * Created by 李震 on 2018/9/27.
  */
private[spark] case class Column (name: String, dataType: Int) {

  def isNumber: Boolean = {
    dataType == Types.INTEGER ||
      dataType == Types.TINYINT ||
      dataType == Types.SMALLINT ||
      dataType ==  Types.BIGINT ||
      dataType ==  Types.FLOAT ||
      dataType ==  Types.DOUBLE ||
      dataType ==  Types.DECIMAL
  }

  def isString: Boolean = {
    dataType == Types.VARCHAR ||
      dataType == Types.NVARCHAR ||
      dataType == Types.CHAR ||
      dataType == Types.NCHAR
  }

  def isDateTime: Boolean = {
    dataType == Types.DATE ||
      dataType == Types.TIMESTAMP
  }

}
