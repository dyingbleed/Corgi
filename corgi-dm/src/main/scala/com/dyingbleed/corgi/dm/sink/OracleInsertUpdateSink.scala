package com.dyingbleed.corgi.dm.sink
import java.sql.Connection

import com.dyingbleed.corgi.core.bean.Column
import com.dyingbleed.corgi.core.util.JDBCUtil
import com.dyingbleed.corgi.core.util.JDBCUtil.WithConnection
import com.dyingbleed.corgi.dm.annotation.EnableSinkOptimization
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.ScalarHandler
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by 李震 on 2019/3/14.
  */
class OracleInsertUpdateSink extends InsertUpdateSink {

  @EnableSinkOptimization
  override def sink(df: DataFrame): Unit = {
    // 配置信息广播变量
    val confB = spark.sparkContext
      .broadcast((conf.url, conf.username, conf.password, conf.sinkDB, conf.sinkTable, conf.pks))

    df.foreachPartition(iterator => {
      val (url, username, password, sinkDB, sinkTable, pks) = confB.value

      JDBCUtil.withAutoClose(url, username, password, new WithConnection {

        override def withConnection(conn: Connection): Unit = {
          val sinkColumnMap = JDBCUtil.getColumns(conn, sinkDB, sinkTable).map(c => (c.getName, c)).toMap // Sink 表字段
          val sinkPKs = JDBCUtil.getPrimaryKey(conn, sinkDB, sinkTable) // Sink 表主键
          val keyColumns = new mutable.ListBuffer[Column] // 更新键
          if (pks.isDefined && pks.get.nonEmpty) {
            for (pk <- pks.get) {
              keyColumns += sinkColumnMap(pk)
            }
          } else {
            // 默认为主键
            keyColumns ++= sinkPKs
          }

          while (iterator.hasNext) { // 主循环，遍历分区
            val row = iterator.next()

            JDBCUtil.withTransaction(conn, new WithConnection {

              override def withConnection(conn: Connection): Unit = {
                val keyBuf = new mutable.ListBuffer[(String, Int, java.lang.Object)] // 索引、字段名、类型、值
                val insertBuf = new mutable.ListBuffer[(String, Int, java.lang.Object)] // 索引、字段名、类型、值
                val updateBuf = new mutable.ListBuffer[(String, Int, java.lang.Object)] // 索引、字段名、类型、值

                var isPK = true // 是否主键插入更新

                for (idx <- row.schema.indices) {
                  val c = sinkColumnMap(row.schema(idx).name)
                  val i = (c.getName, c.getType.toInt, row.getAs[java.lang.Object](idx))

                  insertBuf += i

                  if (keyColumns.contains(c)) {
                    keyBuf += i
                    isPK &&= sinkPKs.contains(c)
                  } else {
                    updateBuf += i
                  }
                }

                if (isPK) {
                  val sql =
                    s"""
                       |BEGIN
                       |  INSERT INTO $sinkDB.$sinkTable (${insertBuf.map(_._1).mkString(",")})
                       |  VALUES (${Array.fill(insertBuf.size)("?").mkString(",")});
                       |EXCEPTION
                       |  WHEN DUP_VAL_ON_INDEX THEN
                       |    UPDATE $sinkDB.$sinkTable
                       |    SET ${updateBuf.map(i => s"${i._1}=?").mkString(",")}
                       |    WHERE ${keyBuf.map(i => s"${i._1}=?").mkString(" AND ")};
                       |END;
                  """.stripMargin

                  new QueryRunner().execute(conn, sql, (insertBuf ++: updateBuf ++: keyBuf).map(_._3):_*)
                } else {
                  val querySQL =
                    s"""
                       |SELECT
                       |  count(*)
                       |FROM $sinkDB.$sinkTable
                       |WHERE ${keyBuf.map(i => s"${i._1}=?").mkString(" AND ")}
                    """.stripMargin

                  val count = new QueryRunner().query(conn, querySQL, new ScalarHandler[java.math.BigDecimal](1), keyBuf.map(_._3):_*)
                  if (count.compareTo(java.math.BigDecimal.ZERO) == 0) {
                    // 插入
                    val insertSQL =
                      s"""
                         |INSERT INTO $sinkDB.$sinkTable (${insertBuf.map(_._1).mkString(",")})
                         |VALUES (${Array.fill(insertBuf.size)("?").mkString(",")})
                      """.stripMargin

                    new QueryRunner().execute(conn, insertSQL, insertBuf.map(_._3):_*)
                  } else {
                    // 更新
                    val updateSQL =
                      s"""
                         |UPDATE $sinkDB.$sinkTable
                         |SET ${updateBuf.map(i => s"${i._1}=?").mkString(",")}
                         |WHERE ${keyBuf.map(i => s"${i._1}=?").mkString(" AND ")}
                      """.stripMargin

                    new QueryRunner().execute(conn, updateSQL, (updateBuf ++: keyBuf).map(_._3):_*)
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
