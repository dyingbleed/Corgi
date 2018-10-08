package com.dyingbleed.corgi.spark.ds.el.split

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDateTime

/**
  * 分区管理器
  *
  * Created by 李震 on 2018/9/27.
  */
trait SplitManager {

  /**
    * 是否可以分区
    *
    * 建议在加载 DataFrame 之前调用此方法
    *
    * @return 是否可以分区
    * */
  def canSplit: Boolean

  /**
    * 加载 DataFrame
    *
    * @return DataFrame
    * */
  def loadDF: DataFrame

}

object SplitManager extends {

  def create(
              spark: SparkSession,
              url: String,
              username: String,
              password: String,
              db: String,
              table: String,
              timeColumn: String,
              executeTime: LocalDateTime
            ): SplitManager = {
    assert(url != null, "数据库 URL 不能为空")
    assert(username != null, "数据库用户名不能为空")
    assert(password != null, "数据库密码不能为空")
    assert(db != null, "数据库名不能为空")
    assert(table != null, "表名不能为空")
    assert(timeColumn != null, "时间字段不能为空")

    if (url.startsWith("jdbc:mysql")) {
      new MySQLSplitManager(spark, url, username, password, db, table, timeColumn, executeTime)
    } else if (url.startsWith("jdbc:oracle:thin")) {
      new OracleSplitManager(spark, url, username, password, db, table, timeColumn, executeTime)
    } else {
      throw new RuntimeException("不支持的数据源")
    }
  }

}
