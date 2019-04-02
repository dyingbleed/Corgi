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
class MySQLSinkTable @Inject() (conf: Conf) extends SinkTable(conf.url, conf.username, conf.password, conf.sinkDB, conf.sinkTable) {

  override def constraints: Seq[Constraint] = {
    // Seq 转 Map
    val columnMap = columns.map(c => (c.getName, c)).toMap

    val sql =
      """
        |SELECT
        |	 tc.CONSTRAINT_NAME,
        |	 tc.CONSTRAINT_TYPE,
        |	 kcu.COLUMN_NAME
        |FROM information_schema.TABLE_CONSTRAINTS tc
        |JOIN information_schema.KEY_COLUMN_USAGE kcu
        |ON tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        |AND tc.TABLE_NAME = kcu.TABLE_NAME
        |and tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        |AND tc.TABLE_SCHEMA = ?
        |AND tc.TABLE_NAME = ?
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
          case "PRIMARY KEY" => c.setType(Constraint.ConstraintType.PRIMARY_KEY)
          case "UNIQUE" => c.setType(Constraint.ConstraintType.UNIQUE)
          case _ =>
        }

        c.setColumns(g._2.map(_.get("COLUMN").asInstanceOf[Column]).toArray)

        c
      })

    r.toSeq
  }

}
