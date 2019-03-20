package com.dyingbleed.corgi.dm.core

import java.sql.Connection
import scala.collection.JavaConversions._

import com.dyingbleed.corgi.core.bean.Column
import com.dyingbleed.corgi.core.util.JDBCUtil

/**
  * Created by 李震 on 2019/3/13.
  */
case class SinkTable(url: String, username: String, password: String, db: String, table: String) {

  private[this] lazy val _conn: Connection = JDBCUtil.getConnection(url, username, password)

  lazy val columns: Seq[Column] = JDBCUtil.getColumns(_conn, db, table).toSeq

  lazy val pks: Seq[Column] = JDBCUtil.getPrimaryKey(_conn, db, table).toSeq

  lazy val cardinality: Long = JDBCUtil.getCardinality(_conn, db, table)

}
