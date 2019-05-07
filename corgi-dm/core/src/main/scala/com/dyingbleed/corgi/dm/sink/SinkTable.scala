package com.dyingbleed.corgi.dm.sink

import java.sql.Connection

import com.dyingbleed.corgi.core.bean.{Column, Constraint}
import com.dyingbleed.corgi.core.util.JDBCUtil

import scala.collection.JavaConversions._
import scala.util.control.Breaks.{break, breakable}

/**
  * Created by 李震 on 2019/3/13.
  */
abstract class SinkTable(url: String, username: String, password: String, db: String, table: String) {

  private[sink] lazy val _conn: Connection = JDBCUtil.getConnection(url, username, password)

  private[sink] lazy val _columnsMap = columns.map(c => (c.getName, c)).toMap

  lazy val columns: Seq[Column] = JDBCUtil.getColumns(_conn, db, table).toSeq

  lazy val pks: Seq[Column] = JDBCUtil.getPrimaryKey(_conn, db, table).toSeq

  lazy val cardinality: Long = JDBCUtil.getCardinality(_conn, db, table)

  def constraints: Seq[Constraint]

  def isConstraintColumn(columns: Seq[Column]): Boolean = {
    var r = false

    val columnSet = columns.toSet
    breakable {
      for (constraint <- constraints) {
        if (constraint.getColumns.toSet.equals(columnSet)) {
          r = true
          break
        }
      }
    }

    r
  }

  def getColumn(name: String): Column = _columnsMap(name)

  def getColumns(names: Seq[String]): Seq[Column] = names.map(_columnsMap(_))

}
