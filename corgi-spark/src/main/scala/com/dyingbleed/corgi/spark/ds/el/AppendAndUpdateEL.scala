package com.dyingbleed.corgi.spark.ds.el

import java.sql.{Connection, DriverManager}

import com.dyingbleed.corgi.spark.core.{Conf, Rpc}
import com.dyingbleed.corgi.spark.ds.{DataSourceEL, DataSourceUtils}
import com.google.inject.Inject
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDateTime

/**
  * Created by 李震 on 2018/6/26.
  */
private[spark] class AppendAndUpdateEL extends DataSourceEL {

  @Inject
  var spark: SparkSession = _

  @Inject
  var conf: Conf = _

  @Inject
  var rpc: Rpc = _

  private val executeTime = LocalDateTime.now()

  /**
    * 判断是否可以分区
    *
    * - 主键
    * - 非联合主键
    * - 数字类型
    *
    * */
  private def checkIfPartition(conn: Connection): Boolean = {
    val dbmd = conn.getMetaData
    val pkrs = dbmd.getPrimaryKeys(conf.sourceDb, null, conf.sourceTable)
    val rsmd = pkrs.getMetaData
    val columnCount = rsmd.getColumnCount

    pkrs.next()
    val primaryKeyColumn = pkrs.getString(4)
    if (pkrs.next()) return false
    pkrs.close()

    val crs = dbmd.getColumns(conf.sourceDb, null, conf.sourceTable, primaryKeyColumn)
    crs.next()
    val dataType = crs.getInt(5)
    crs.close()

    dataType == 4 || // INTEGER
      dataType == -6 || // TINYINT
      dataType == 5 || // SMALLINT
      dataType ==  -5 || // BIGINT
      dataType ==  7 || // FLOAT
      dataType ==  8 || // DOUBLE
      dataType ==  3 // DECIMAL
  }

  private def partitionColumn(conn: Connection): String = {
    val dbmd = conn.getMetaData
    val pkrs = dbmd.getPrimaryKeys(conf.sourceDb, null, conf.sourceTable)
    val rsmd = pkrs.getMetaData
    val columnCount = rsmd.getColumnCount

    pkrs.next()
    val primaryKeyColumn = pkrs.getString(4)
    pkrs.close()

    primaryKeyColumn
  }

  private def partitionStats(conn: Connection, column: String): (Int, Int, Int) = {
    val stat = conn.createStatement()

    val rs = stat.executeQuery(
      s"""
         |select
         |  min(${column}) as lowerbound,
         |  max(${column}) as upperbound,
         |  count(1) as count
         |from ${conf.sourceDb}.${conf.sourceTable}
    """.stripMargin)
    rs.next()

    val lowerBound = rs.getInt(1)
    val upperBound = rs.getInt(2)
    val numPatitions = (rs.getInt(3) / 1000000) + 1

    rs.close()
    stat.close()

    (lowerBound, upperBound, numPatitions)
  }

  private def getPatitionInfo(): Option[(String, Int, Int, Int)] = {
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection(conf.sourceDbUrl, conf.sourceDbUser, conf.sourceDbPassword)

    /*
     * 1.检查是否满足分区条件
     * */
    if (!checkIfPartition(conn)) return None

    /*
     * 2.分区列
     * */
    val column = partitionColumn(conn)

    /*
     * 3.分区统计信息
     * */
    val stats = partitionStats(conn, column)

    conn.close()

    Option((column, stats._1, stats._2, stats._3))
  }

  /**
    * 加载数据源
    *
    * @return
    **/
  override def loadSourceDF: DataFrame = {
    if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable)) {
      // 全量
      val partitionInfo = getPatitionInfo()

      val table =
        s"""
           |(select
           |  *, date(${conf.sourceTimeColumn}) as ods_date
           |from ${conf.sourceDb}.${conf.sourceTable}
           |where ${conf.sourceTimeColumn} < '${executeTime.toString("yyyy-MM-dd HH:mm:ss")}'
           |) t
          """.stripMargin

      val reader = spark.read
        .format("jdbc")
        .option("url", conf.sourceDbUrl)
        .option("dbtable", table)
        .option("user", conf.sourceDbUser)
        .option("password", conf.sourceDbPassword)
        .option("driver", "com.mysql.jdbc.Driver")

      (if (partitionInfo.isDefined) {
        val info = partitionInfo.get
        reader.option("partitionColumn", info._1)
          .option("lowerBound", info._2)
          .option("upperBound", info._3)
          .option("numPartitions", info._4)
      } else reader).load()

    } else {
      // 增量
      val table =
        s"""
           |(select
           |  *
           |from ${conf.sourceDb}.${conf.sourceTable}
           |where ${conf.sourceTimeColumn} >= '${rpc.getLastExecuteTime.toString("yyyy-MM-dd HH:mm:ss")}'
           |and ${conf.sourceTimeColumn} < '${executeTime.toString("yyyy-MM-dd HH:mm:ss")}'
           |) t
         """.stripMargin

      spark.read
        .format("jdbc")
        .option("url", conf.sourceDbUrl)
        .option("dbtable", table)
        .option("user", conf.sourceDbUser)
        .option("password", conf.sourceDbPassword)
        .option("driver", "com.mysql.jdbc.Driver")
        .load()
    }
  }

  /**
    * 持久化数据源
    *
    * @return
    **/
  override def persistSinkDF(df: DataFrame): Unit = {
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
           |add if not exists partition (ods_date='${executeTime.toString("yyyy-MM-dd")}')
          """.stripMargin)

      DataSourceUtils.forceInsertOverwriteTablePartition(df, conf.sinkDb, conf.sinkTable, executeTime.toLocalDate)
    }

    rpc.saveExecuteTime(executeTime) // 保存执行时间戳
  }
}
