package com.dyingbleed.corgi.dm.sink

import java.sql.{Connection, PreparedStatement}

import com.dyingbleed.corgi.core.util.JDBCUtil
import com.dyingbleed.corgi.core.util.JDBCUtil.WithConnection
import com.dyingbleed.corgi.dm.annotation.EnableSinkOptimization
import com.dyingbleed.corgi.dm.core.{Conf, SinkTable}
import com.google.inject.Inject
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * Created by 李震 on 2019/3/12.
  */
class InsertOverwriteSink extends Sink {

  @Inject
  var conf: Conf = _

  @Inject
  var sinkTable: SinkTable = _

  @Inject
  var spark: SparkSession = _

  @EnableSinkOptimization
  override def sink(df: DataFrame): Unit = {
    if (sinkTable.cardinality > 1000) {
      /*
       * truncate
       * */
      df.write
        .format("jdbc")
        .mode(SaveMode.Overwrite)
        .option("url", conf.url)
        .option("driver", conf.sinkVendor.getDriverClassName)
        .option("dbtable", s"${conf.sinkDB}.${conf.sinkTable}")
        .option("user", conf.username)
        .option("password", conf.password)
        .option("truncate", true)
        .save()
    } else {
      /*
       * transactional delete
       * */
      val confB = spark.sparkContext.broadcast((conf.url, conf.username, conf.password, conf.sinkDB, conf.sinkTable))

      df.repartition(1).foreachPartition(iterator => {
        val (url, username, password, sinkDB, sinkTable) = confB.value

        JDBCUtil.withAutoClose(url, username, password, new WithConnection {

          override def withConnection(conn: Connection): Unit = {
            val sinkColumns = JDBCUtil.getColumns(conn, sinkDB, sinkTable).toSeq

            JDBCUtil.withTransaction(conn, new WithConnection {

              override def withConnection(conn: Connection): Unit = {
                /* *****
                 * 删除
                 * *****/
                val del = conn.createStatement()
                del.execute(s"DELETE FROM $sinkDB.$sinkTable")

                /* *********
                 * 批量插入
                 * *********/
                var batchInsertStat: PreparedStatement = null
                while (iterator.hasNext) { // 主循环，遍历分区
                  val row = iterator.next()

                  val inserts = new mutable.ListBuffer[(Int, String, Int, Any)] // 索引、字段名、类型、值

                  for (idx <- row.schema.indices) {
                    val field = row.schema(idx)
                    breakable {
                      for (c <- sinkColumns) {
                        if (c.getName.equalsIgnoreCase(field.name)) {
                          val i = (idx, c.getName, c.getType.toInt, row.get(idx))
                          inserts += i
                          break
                        }
                      }
                    }
                  }

                  if (batchInsertStat == null) {
                    val sql = s"""
                                 |INSERT INTO $sinkDB.$sinkTable
                                 |(${inserts.map(_._2).mkString(",")})
                                 |VALUES
                                 |(${Array.fill(inserts.size)("?").mkString(",")})
                              """.stripMargin
                    batchInsertStat = conn.prepareStatement(sql)
                  }

                  for (i <- inserts.indices) {
                    val insert = inserts(i)
                    batchInsertStat.setObject(i + 1, insert._4, insert._3)
                  }

                  batchInsertStat.addBatch()
                }

                batchInsertStat.executeBatch()
              }

            })
          }

        })
      })
    }
  }

}
