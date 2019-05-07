package com.dyingbleed.corgi.dm.sink

import com.dyingbleed.corgi.core.bean.{Column, Constraint}
import com.dyingbleed.corgi.dm.core.Conf
import com.google.inject.Inject
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.MapListHandler

import scala.collection.JavaConversions._

/**
  * Created by 李震 on 2019/4/1.
  */
class OracleSinkTable @Inject() (conf: Conf) extends SinkTable(conf.url, conf.username, conf.password, conf.sinkDB, conf.sinkTable) {

  override def constraints: Seq[Constraint] = {
    // Seq 转 Map
    val columnMap = columns.map(c => (c.getName, c)).toMap

    val sql =
      """
        |SELECT
        |	ac.CONSTRAINT_NAME,
        |	ac.CONSTRAINT_TYPE,
        |	acc.COLUMN_NAME
        |FROM ALL_CONSTRAINTS ac
        |JOIN ALL_CONS_COLUMNS acc
        |ON ac.OWNER = acc.OWNER
        |AND ac.TABLE_NAME = acc.TABLE_NAME
        |AND ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
        |AND ac.OWNER = ?
        |AND ac.TABLE_NAME = ?
      """.stripMargin

    val r = new QueryRunner()
      .execute(_conn, sql, new MapListHandler(), conf.sinkDB, conf.sinkTable)
      .flatMap(_.toSeq)
      .map(m => {
        m.put("COLUMN", columnMap.get(m.get("COLUMN_NAME")))
        m
      })
      .groupBy(m => (m.get("CONSTRAINT_NAME"), m.get("CONSTRAINT_TYPE")))
      .map(g => {
        val c = new Constraint()

        c.setName(g._1._1.asInstanceOf[String])
        g._1._2 match {
          case "P" => c.setType(Constraint.ConstraintType.PRIMARY_KEY)
          case "U" => c.setType(Constraint.ConstraintType.UNIQUE)
          case _ =>
        }

        c.setColumns(g._2.map(_.get("COLUMN").asInstanceOf[Column]).toArray)

        c
      })

    r.toSeq
  }

}
