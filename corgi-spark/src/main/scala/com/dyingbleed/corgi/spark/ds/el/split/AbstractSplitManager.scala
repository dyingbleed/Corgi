package com.dyingbleed.corgi.spark.ds.el.split

import com.dyingbleed.corgi.spark.bean.{Column, Table}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by 李震 on 2018/9/27.
  */
private[split] abstract class AbstractSplitManager(spark: SparkSession, table: Table) extends SplitManager {

  private var isSinglePKNum: Option[Boolean] = None // 表是否为单一数值型主键
  private var isPKStr: Option[Boolean] = None // 表是否为字符型主键

  /**
    * 是否可以分区
    *
    * @return 是否可以分区
    **/
  override def canSplit: Boolean = {
    if (isSinglePKNum.isEmpty || isPKStr.isEmpty) {
      val pk = table.pk
      // 只有一个主键且为数值型
      isSinglePKNum = Option(pk.get.size == 1 && pk.get.last.isNumber)
      // 有多个主键且为数值型或字符型
      isPKStr = Option(pk.get.nonEmpty && pk.get.forall(col => col.isNumber || col.isString))
    }

    isSinglePKNum.getOrElse(false) || isPKStr.getOrElse(false)
  }

  /**
    * 加载 DataFrame
    *
    * @return DataFrame
    **/
  override def loadDF: DataFrame = {
    val parallellism = spark.conf.get("spark.sql.shuffle.partitions", "200").toLong

    if (canSplit) {
      val pk = table.pk
      if (isSinglePKNum.get) {
        val pkColumnName = pk.get.last.name
        val stats = table.stat[Long, Long](pkColumnName, classOf[Long], classOf[Long])
        return getDF(pk.get.last, stats.max, stats.min, Math.min((stats.cardinality / 10000) + 1, parallellism))
      } else if (isPKStr.get) {
        return getDF(pk.get, parallellism)
      }
    }

    null
  }

  /**
    * 获取 DataFrame
    *
    * @param splitBy 分区字段
    * @param upper 值上界
    * @param lower 值下界
    * @param m 并发度
    *
    * @return DataFrame
    * */
  def getDF(splitBy: Column, upper: Long, lower: Long, m: Long): DataFrame

  /**
    * 获取 DataFrame
    *
    * @param splitBy 分区字段
    * @param m 并发度
    *
    * @return DataFrame
    * */
  def getDF(splitBy: Seq[Column], m: Long): DataFrame

}
