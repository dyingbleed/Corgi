package com.dyingbleed.corgi.dm.sink
import java.sql.Connection

import com.dyingbleed.corgi.core.util.JDBCUtil
import com.dyingbleed.corgi.core.util.JDBCUtil.WithConnection
import com.dyingbleed.corgi.dm.annotation.EnableSinkOptimization
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}
import scala.collection.JavaConversions._

/**
  * Created by 李震 on 2019/3/14.
  */
class MySQLInsertUpdateSink extends InsertUpdateSink {

  @EnableSinkOptimization
  override def sink(df: DataFrame): Unit = {
    // 配置信息广播变量
    val confB = spark.sparkContext
      .broadcast((conf.url, conf.username, conf.password, conf.sinkDB, conf.sinkTable))

    df.foreachPartition(iterator => {
      val (url, username, password, sinkDB, sinkTable) = confB.value

      JDBCUtil.withAutoClose(url, username, password, new WithConnection {

        override def withConnection(conn: Connection): Unit = {
          val sinkPKs = JDBCUtil.getPrimaryKey(conn, sinkDB, sinkTable) // Sink 表主键
          val sinkColumns = JDBCUtil.getColumns(conn, sinkDB, sinkTable) // Sink 表字段

          while (iterator.hasNext) { // 主循环，遍历分区
            val row = iterator.next()

            JDBCUtil.withTransaction(conn, new WithConnection {

              override def withConnection(conn: Connection): Unit = {
                val inserts = new mutable.ListBuffer[(Int, String, Int, Any)] // 索引、字段名、类型、值
                val updates = new mutable.ListBuffer[(Int, String, Int, Any)] // 索引、字段名、类型、值

                for (idx <- row.schema.indices) {
                  val field = row.schema(idx)
                  breakable {
                    for (c <- sinkColumns) {
                      if (c.getName.equalsIgnoreCase(field.name)) {
                        val i = (idx, c.getName, c.getType.toInt, row.get(idx))
                        inserts += i
                        if (!sinkPKs.contains(c)) {
                          updates += i
                        }
                        break
                      }
                    }
                  }
                }

                val sql = s"""
                             |INSERT INTO $sinkDB.$sinkTable (${inserts.map(_._2).mkString(",")})
                             |VALUES (${Array.fill(inserts.size)("?").mkString(",")})
                             |ON DUPLICATE KEY
                             |UPDATE ${updates.map(i => s"${i._2}=?").mkString(",")}
                          """.stripMargin
                val insertStat = conn.prepareStatement(sql)

                for (i <- inserts.indices) {
                  val insert = inserts(i)
                  insertStat.setObject(i + 1, insert._4, insert._3)
                }
                for (j <- updates.indices) {
                  val update = updates(j)
                  insertStat.setObject(j + inserts.size + 1, update._4, update._3)
                }

                insertStat.execute()
                insertStat.close()
              }

            })
          }
        }

      })
    })
  }

}
