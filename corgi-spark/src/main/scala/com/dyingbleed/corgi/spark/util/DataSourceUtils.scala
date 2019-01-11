package com.dyingbleed.corgi.spark.util

import com.dyingbleed.corgi.spark.core.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDate

/**
  * Created by 李震 on 2018/6/26.
  */
object DataSourceUtils {

  /**
    * 创建并插入 Hive 表
    * @param df
    * @param dbName 数据库名
    * @param tableName 表名
    * @param partitionColumns 分区字段
    * */
  def createAndInsertHiveTable(df: DataFrame, dbName: String, tableName: String, partitionColumns: Array[String]): Unit = {
    df.createOrReplaceTempView("sink")

    // 创建表
    df.sparkSession.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $dbName.$tableName
         |USING PARQUET
         |PARTITIONED BY (${partitionColumns.mkString(",")})
         |AS SELECT * FROM sink
            """.stripMargin)
  }

  /**
    * 创建 Hive 表
    * @param df
    * @param dbName 数据库名
    * @param tableName 表名
    * @param partitionColumns 分区字段
    * */
  def createHiveTable(df: DataFrame, dbName: String, tableName: String, partitionColumns: Array[String]): Unit = {
    df.createOrReplaceTempView("sink")

    // 创建表
    df.sparkSession.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $dbName.$tableName
         |USING PARQUET
         |PARTITIONED BY (${partitionColumns.mkString(",")})
         |AS SELECT * FROM sink WHERE 1=0
            """.stripMargin)
  }

  /**
    * 向 Hive 表插入数据
    * @param df
    * @param dbName 数据库名
    * @param tableName 表名
    * @param date 日期
    * @param partitionColumns 分区字段
    * */
  def insertHiveTable(df: DataFrame, dbName: String, tableName: String, date: LocalDate, partitionColumns: Array[String]): Unit = {
    df.createOrReplaceTempView("sink")

    // 插入数据
    df.sparkSession.sql(
      s"""
         |INSERT OVERWRITE TABLE $dbName.$tableName
         |PARTITION(${Constants.DATE_PARTITION}='${date.toString("yyyy-MM-dd")}'${partitionColumns.filter(c => !Constants.DATE_PARTITION.equalsIgnoreCase(c)).map(c => s", $c").mkString})
         |SELECT * FROM sink
            """.stripMargin)
  }

  private[this] def withHive(f: Hive => Unit): Unit = {
    val conf = new Configuration()
    conf.set("hive.metastore.uris", System.getProperty("hive.metastore.uris"))
    val hiveConf = new HiveConf(conf, classOf[HiveConf])
    val hive = Hive.get(hiveConf)

    f(hive)
  }

}
