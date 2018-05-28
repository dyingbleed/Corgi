package com.dyingbleed.corgi.spark.core

import java.sql.DriverManager
import scala.collection.JavaConversions._

import com.google.inject.Inject
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.LocalDateTime

import com.dyingbleed.corgi.spark.core.ODSMode._

/**
  * Created by 李震 on 2018/3/2.
  */
class DataSource {
  
  @Inject
  var spark: SparkSession = _
  
  @Inject
  var conf: Conf = _

  @Inject
  var metadata: Metadata = _

  private val timestamp: LocalDateTime = LocalDateTime.now() // 当前读入数据时间戳

  private def getSourceStats(): (Int, Int, Int) = {
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection(conf.dbUrl, conf.dbUser, conf.dbPassword)
    val stat = conn.createStatement()

    val rs = stat.executeQuery(s"""
      |select
      |  min(id) as lowerbound,
      |  max(id) as upperbound
      |from ${conf.hiveTable}
    """.stripMargin)
    rs.next()

    val lowerBound = rs.getInt(1)
    val upperBound = rs.getInt(2)
    val numPatitions = ((upperBound - lowerBound) / 1000000) + 1

    stat.close()
    conn.close()

    (lowerBound, upperBound, numPatitions)
  }

  private def getTable(ts: LocalDateTime): String = {
    conf.mode match {
      case COMPLETE => {
        // 全量
        conf.dbTable
      }
      case APPEND | UPDATE => {
        if (!spark.catalog.tableExists(conf.hiveDB, conf.hiveTable)) {
          // 全量
          s"""
            |(select
            |  *, date(${conf.modifyTimeColumn}) as ods_date
            |from ${conf.dbTable}
            |where ${conf.modifyTimeColumn} < '${ts.toString("yyyy-MM-dd HH:mm:ss")}'
            |) t
          """.stripMargin
        } else {
          // 增量
          s"""
             |(select
             |  *
             |from ${conf.dbTable}
             |where ${conf.modifyTimeColumn} >= '${metadata.getLastModifyDate.toString("yyyy-MM-dd HH:mm:ss")}'
             |and ${conf.modifyTimeColumn} < '${ts.toString("yyyy-MM-dd HH:mm:ss")}'
             |) t
         """.stripMargin
        }
      }
    }
  }

  private def getTableColumns(databaseName: String, tableName: String): List[Column] = {
    spark.catalog.listColumns(databaseName, tableName).collectAsList().toList
  }

  def loadSourceDF: DataFrame = {
    conf.mode match {
      case COMPLETE => {
        // 全量
        spark.read
          .format("jdbc")
          .option("url", conf.dbUrl)
          .option("dbtable", getTable(timestamp))
          .option("user", conf.dbUser)
          .option("password", conf.dbPassword)
          .option("driver", "com.mysql.jdbc.Driver")
          .load()
      }
      case APPEND | UPDATE => {
        if (!spark.catalog.tableExists(conf.hiveDB, conf.hiveTable)) {
          // 全量
          val sourceStats = getSourceStats()

          spark.read
            .format("jdbc")
            .option("url", conf.dbUrl)
            .option("dbtable", getTable(timestamp))
            .option("user", conf.dbUser)
            .option("password", conf.dbPassword)
            .option("driver", "com.mysql.jdbc.Driver")
            .option("partitionColumn", "id")
            .option("lowerBound", sourceStats._1)
            .option("upperBound", sourceStats._2)
            .option("numPartitions", sourceStats._3)
            .load()
        } else {
          // 增量
          spark.read
            .format("jdbc")
            .option("url", conf.dbUrl)
            .option("dbtable", getTable(timestamp))
            .option("user", conf.dbUser)
            .option("password", conf.dbPassword)
            .option("driver", "com.mysql.jdbc.Driver")
            .load()
        }
      }
    }
  }

  def persistSinkDF(df: DataFrame): Unit = {
    conf.mode match {
      case COMPLETE => {
        // 创建表
        val schema = df.schema.add("ods_date", StringType)
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema).createOrReplaceTempView("t")
        spark.sql(
          s"""
            |create table if not exists ${conf.hiveDB}.${conf.hiveTable}
            |using PARQUET
            |partitioned by (ods_date)
            |as select * from t
          """.stripMargin
        )

        // 增加分区
        spark.sql(
          s"""
            |alter table ${conf.hiveDB}.${conf.hiveTable}
            |add if not exists partition (ods_date='${timestamp.toString("yyyy-MM-dd")}')
          """.stripMargin)

        // 插入数据
        // 静态分区
        df.createOrReplaceTempView("sink")
        spark.sql(
          s"""
            |insert overwrite table ${conf.hiveDB}.${conf.hiveTable}
            |partition(ods_date='${timestamp.toString("yyyy-MM-dd")}')
            |select
            |    ${getTableColumns(conf.hiveDB, conf.hiveTable).map(_.name).filter(!_.equals("ods_date")).mkString(",")}
            |from sink
          """.stripMargin)
      }
      case APPEND | UPDATE => {
        if (!spark.catalog.tableExists(conf.hiveDB, conf.hiveTable)) {
          df.createOrReplaceTempView("sink")

          // 创建表
          spark.sql(
            s"""
              |create table if not exists ${conf.hiveDB}.${conf.hiveTable}
              |using PARQUET
              |partitioned by (ods_date)
              |as select * from sink
            """.stripMargin
          )

          // 插入数据
          // 动态分区
          spark.sql(
            s"""
              |insert overwrite table ${conf.hiveDB}.${conf.hiveTable}
              |partition(ods_date)
              |select * from sink
            """.stripMargin)
        } else {
          df.createOrReplaceTempView("sink")

          // 插入数据
          // 静态分区
          spark.sql(
            s"""
              |insert overwrite table ${conf.hiveDB}.${conf.hiveTable}
              |partition(ods_date='${timestamp.toString("yyyy-MM-dd")}')
              |select
              |  ${getTableColumns(conf.hiveDB, conf.hiveTable).map(_.name).filter(!_.equals("ods_date")).mkString(",")}
              |from sink
            """.stripMargin)
        }
      }
    }

    metadata.saveLastModifyDate(timestamp) // 保存执行时间戳
  }

}
