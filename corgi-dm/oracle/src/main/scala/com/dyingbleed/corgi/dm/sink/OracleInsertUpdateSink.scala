package com.dyingbleed.corgi.dm.sink
import java.sql.Connection

import com.dyingbleed.corgi.core.util.JDBCUtil
import com.dyingbleed.corgi.core.util.JDBCUtil.WithConnection
import com.dyingbleed.corgi.dm.annotation.EnableSinkOptimization
import com.dyingbleed.corgi.dm.core.Conf
import com.google.inject.Inject
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.ScalarHandler
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * Created by 李震 on 2019/3/14.
  */
class OracleInsertUpdateSink extends Sink {

  @Inject
  var conf: Conf = _

  @Inject
  var sinkTable: SinkTable = _

  @Inject
  var spark: SparkSession = _

  @EnableSinkOptimization
  override def sink(df: DataFrame): Unit = {
    // 📢 Sink 表配置
    val confBroadcast = spark.sparkContext
      .broadcast((conf.url, conf.username, conf.password, conf.sinkDB, conf.sinkTable))
    // 📢 Sink 表字段
    val columnsBroadcast = spark.sparkContext.broadcast(sinkTable.columns)

    // 📢 Sink 表更新字段
    val keyColumns = if (conf.pks.isDefined && conf.pks.get.nonEmpty) {
      sinkTable.getColumns(conf.pks.get)
    } else {
      // 默认为主键
      sinkTable.pks
    }
    val keyColumnsBroadcast = spark.sparkContext.broadcast(keyColumns)

    // 📢 是否 Constraint 字段
    val isConstraintColumnBroadcast = spark.sparkContext
      .broadcast(sinkTable.isConstraintColumn(keyColumns))

    df.foreachPartition(iterator => {
      val (url, username, password, sinkDB, sinkTable) = confBroadcast.value

      JDBCUtil.withAutoClose(url, username, password, new WithConnection {

        override def withConnection(conn: Connection): Unit = {
          val sinkColumnMap = columnsBroadcast.value.map(c => (c.getName, c)).toMap // Sink 表字段
          val keyColumns = keyColumnsBroadcast.value

          while (iterator.hasNext) { // 主循环，遍历分区
            val row = iterator.next()

            JDBCUtil.withTransaction(conn, new WithConnection {

              override def withConnection(conn: Connection): Unit = {
                val keys = new mutable.ListBuffer[(String, Int, java.lang.Object)] // 索引、字段名、类型、值
                val inserts = new mutable.ListBuffer[(String, Int, java.lang.Object)] // 索引、字段名、类型、值
                val updates = new mutable.ListBuffer[(String, Int, java.lang.Object)] // 索引、字段名、类型、值

                for (idx <- row.schema.indices) {
                  val c = sinkColumnMap(row.schema(idx).name)
                  val i = (c.getName, c.getType.toInt, row.getAs[java.lang.Object](idx))

                  inserts += i

                  if (keyColumns.contains(c)) {
                    keys += i
                  } else {
                    updates += i
                  }
                }

                if (isConstraintColumnBroadcast.value) {
                  val sql =
                    s"""
                       |BEGIN
                       |  INSERT INTO $sinkDB.$sinkTable (${inserts.map(_._1).mkString(",")})
                       |  VALUES (${Array.fill(inserts.size)("?").mkString(",")});
                       |EXCEPTION
                       |  WHEN DUP_VAL_ON_INDEX THEN
                       |    UPDATE $sinkDB.$sinkTable
                       |    SET ${updates.map(i => s"${i._1}=?").mkString(",")}
                       |    WHERE ${keys.map(i => s"${i._1}=?").mkString(" AND ")};
                       |END;
                  """.stripMargin

                  new QueryRunner().execute(conn, sql, (inserts ++: updates ++: keys).map(_._3):_*)
                } else {
                  val querySQL =
                    s"""
                       |SELECT
                       |  count(*)
                       |FROM $sinkDB.$sinkTable
                       |WHERE ${keys.map(i => s"${i._1}=?").mkString(" AND ")}
                    """.stripMargin

                  val count = new QueryRunner().query(conn, querySQL, new ScalarHandler[java.math.BigDecimal](1), keys.map(_._3):_*)
                  if (count.compareTo(java.math.BigDecimal.ZERO) == 0) {
                    // 插入
                    val insertSQL =
                      s"""
                         |INSERT INTO $sinkDB.$sinkTable (${inserts.map(_._1).mkString(",")})
                         |VALUES (${Array.fill(inserts.size)("?").mkString(",")})
                      """.stripMargin

                    new QueryRunner().execute(conn, insertSQL, inserts.map(_._3):_*)
                  } else {
                    // 更新
                    val updateSQL =
                      s"""
                         |UPDATE $sinkDB.$sinkTable
                         |SET ${updates.map(i => s"${i._1}=?").mkString(",")}
                         |WHERE ${keys.map(i => s"${i._1}=?").mkString(" AND ")}
                      """.stripMargin

                    new QueryRunner().execute(conn, updateSQL, (updates ++: keys).map(_._3):_*)
                  }
                }
              }

            })
          }
        }

      })
    })
  }

}
