package com.dyingbleed.corgi.spark.ds

import java.sql.DriverManager

import com.dyingbleed.corgi.spark.core.{Conf, Rpc}
import com.dyingbleed.corgi.spark.core.ODSMode._
import com.google.inject.Inject
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.joda.time.{LocalDate, LocalDateTime}

import scala.collection.JavaConversions._

/**
  * Created by 李震 on 2018/3/2.
  */
private[spark] class DataSource {
  
  @Inject
  var spark: SparkSession = _
  
  @Inject
  var conf: Conf = _

  @Inject
  var rpc: Rpc = _

  private val timestamp: LocalDateTime = LocalDateTime.now() // 当前读入数据时间戳

  private def getSourceStats(): (Int, Int, Int) = {
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection(conf.sourceDbUrl, conf.sourceDbUser, conf.sourceDbPassword)
    val stat = conn.createStatement()

    val rs = stat.executeQuery(s"""
      |select
      |  min(id) as lowerbound,
      |  max(id) as upperbound
      |from ${conf.sourceDb}.${conf.sourceTable}
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
        conf.sourceTable
      }
      case APPEND | UPDATE => {
        if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable)) {
          // 全量
          s"""
            |(select
            |  *, date(${conf.sourceTimeColumn}) as ods_date
            |from ${conf.sourceDb}.${conf.sourceTable}
            |where ${conf.sourceTimeColumn} < '${ts.toString("yyyy-MM-dd HH:mm:ss")}'
            |) t
          """.stripMargin
        } else {
          // 增量
          s"""
             |(select
             |  *
             |from ${conf.sourceDb}.${conf.sourceTable}
             |where ${conf.sourceTimeColumn} >= '${rpc.getLastExecuteTime.toString("yyyy-MM-dd HH:mm:ss")}'
             |and ${conf.sourceTimeColumn} < '${ts.toString("yyyy-MM-dd HH:mm:ss")}'
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
          .option("url", conf.sourceDbUrl)
          .option("dbtable", getTable(timestamp))
          .option("user", conf.sourceDbUser)
          .option("password", conf.sourceDbPassword)
          .option("driver", "com.mysql.jdbc.Driver")
          .load()
      }
      case APPEND | UPDATE => {
        if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable)) {
          // 全量
          val sourceStats = getSourceStats()

          spark.read
            .format("jdbc")
            .option("url", conf.sourceDbUrl)
            .option("dbtable", getTable(timestamp))
            .option("user", conf.sourceDbUser)
            .option("password", conf.sourceDbPassword)
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
            .option("url", conf.sourceDbUrl)
            .option("dbtable", getTable(timestamp))
            .option("user", conf.sourceDbUser)
            .option("password", conf.sourceDbPassword)
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
            |create table if not exists ${conf.sinkDb}.${conf.sinkTable}
            |using PARQUET
            |partitioned by (ods_date)
            |as select * from t
          """.stripMargin
        )

        // 增加分区
        spark.sql(
          s"""
            |alter table ${conf.sinkDb}.${conf.sinkTable}
            |add if not exists partition (ods_date='${timestamp.toString("yyyy-MM-dd")}')
          """.stripMargin)

        // 插入数据
        // 静态分区
        forceInsertOverwriteTablePartition(df, conf.sinkDb, conf.sinkTable, timestamp.toLocalDate)
      }
      case APPEND | UPDATE => {
        if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable)) {
          df.createOrReplaceTempView("sink")

          // 创建表
          spark.sql(
            s"""
              |create table if not exists ${conf.sinkDb}.${conf.sinkTable}
              |using PARQUET
              |partitioned by (ods_date)
              |as select * from sink
            """.stripMargin
          )

          // 插入数据
          // 动态分区
          spark.sql(
            s"""
              |insert overwrite table ${conf.sinkDb}.${conf.sinkTable}
              |partition(ods_date)
              |select * from sink
            """.stripMargin)
        } else {
          // 增加分区
          spark.sql(
            s"""
               |alter table ${conf.sinkDb}.${conf.sinkTable}
               |add if not exists partition (ods_date='${timestamp.toString("yyyy-MM-dd")}')
          """.stripMargin)

          forceInsertOverwriteTablePartition(df, conf.sinkDb, conf.sinkTable, timestamp.toLocalDate)
        }
      }
    }

    rpc.saveExecuteTime(timestamp) // 保存执行时间戳
  }

  /**
    * 强制插入覆盖到表分区
    *
    * @param df
    * @param db
    * @param table
    * @param date
    *
    * */
  private def forceInsertOverwriteTablePartition(df: DataFrame, db: String, table: String, date: LocalDate): Unit = {
    val location = spark.sql(s"desc formatted ${db}.${table}").filter("col_name like 'Location%'").collect().last.getString(1)
    val path = s"${location}/ods_date=${date.toString("yyyy-MM-dd")}"
    df.coalesce(8).write.mode(SaveMode.Overwrite).parquet(path)
  }

}
