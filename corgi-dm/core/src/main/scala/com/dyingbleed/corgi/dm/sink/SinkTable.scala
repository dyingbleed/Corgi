package com.dyingbleed.corgi.dm.sink

import java.sql.Connection

import com.dyingbleed.corgi.core.bean.{Column, Constraint}
import com.dyingbleed.corgi.core.util.JDBCUtil

import scala.collection.JavaConversions._

/**
  * Created by 李震 on 2019/3/13.
  */
abstract class SinkTable(url: String, username: String, password: String, db: String, table: String) {

  private[sink] lazy val _conn: Connection = JDBCUtil.getConnection(url, username, password)

  lazy val columns: Seq[Column] = JDBCUtil.getColumns(_conn, db, table).toSeq

  lazy val pks: Seq[Column] = JDBCUtil.getPrimaryKey(_conn, db, table).toSeq

  lazy val cardinality: Long = JDBCUtil.getCardinality(_conn, db, table)

  def constraints: Seq[Constraint]

}
