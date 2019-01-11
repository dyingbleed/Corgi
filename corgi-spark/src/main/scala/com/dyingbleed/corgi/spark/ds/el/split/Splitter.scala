package com.dyingbleed.corgi.spark.ds.el.split

import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2019/1/8.
  */
trait Splitter {

  /* *********
   * 公共方法 *
   * *********/

  def canSplit: Boolean

  protected def loadPKRangeSplitDF: DataFrame

  protected def loadPKHashSplitDF: DataFrame

  protected def loadPartitionSplitDF: DataFrame

  /* *********
   * 工具方法 *
   * *********/

  protected def cast2Long(v: Any): Long = {
    v match {
      case l:Long => l
      case s:Short => s.toLong
      case i:Int => i.toLong
      case f:Float => f.toLong
      case d:Double => d.toLong
      case _ => throw new RuntimeException(s"不支持的数据类型")
    }
  }

}