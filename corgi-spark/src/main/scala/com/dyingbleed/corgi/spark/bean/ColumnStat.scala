package com.dyingbleed.corgi.spark.bean

/**
  * Created by 李震 on 2018/11/29.
  */
case class ColumnStat[MAX, MIN](max: MAX, min: MIN, cardinality: Long)
