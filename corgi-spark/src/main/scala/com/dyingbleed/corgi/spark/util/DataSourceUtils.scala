package com.dyingbleed.corgi.spark.util

import com.dyingbleed.corgi.spark.core.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.{Hive, Table}
import org.apache.spark.sql.{DataFrame, SaveMode}
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
    * @param partitionColumns 分区字段
    * */
  def insertHiveTable(df: DataFrame, dbName: String, tableName: String, partitionColumns: Array[String]): Unit = {
    df.createOrReplaceTempView("sink")

    // 插入数据
    df.sparkSession.sql(
      s"""
         |INSERT INTO TABLE $dbName.$tableName
         |PARTITION(${partitionColumns.mkString(",")})
         |SELECT * FROM sink
            """.stripMargin)
  }

  /**
    * 强制插入覆盖到表
    *
    * @param df
    * @param db
    * @param table
    * @param date
    *
    * */
  def forceInsertOverwriteTablePartition(df: DataFrame, db: String, table: String, date: LocalDate): Unit = withHive(hive => {
    val tableMeta = hive.getTable(db, table)
    val location = tableMeta.getDataLocation
    val partitionColumnName = getPartColName(tableMeta)

    val path = s"$location/$partitionColumnName=${date.toString(Constants.DATE_FORMAT)}"
    df.write.mode(SaveMode.Overwrite).parquet(path)
  })

  private[this] def withHive(f: Hive => Unit): Unit = {
    val conf = new Configuration()
    conf.set("hive.metastore.uris", System.getProperty("hive.metastore.uris"))
    val hiveConf = new HiveConf(conf, classOf[HiveConf])
    val hive = Hive.get(hiveConf)

    f(hive)
  }

  private[this] def getPartColName(tableMeta: Table): String = {
    val originColName = tableMeta.getPartCols.get(0).getName
    val sparkColName = tableMeta.getParameters.get("spark.sql.sources.schema.partCol.0")

    if (sparkColName != null) sparkColName else originColName
  }

}
