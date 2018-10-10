package com.dyingbleed.corgi.spark.util

import java.sql.Connection

import com.dyingbleed.corgi.spark.bean.Column

import scala.collection.mutable.ListBuffer

/**
  * Created by 李震 on 2018/9/27.
  */
object JDBCUtils {

  /**
    * 获取主键信息
    *
    * @param conn 数据库连接
    * @param db 数据库名
    * @param table 表名
    *
    * @return 主键信息
    * */
  def getPrimaryKey(conn: Connection, db: String, table: String): Seq[Column] = {
    val pks = new ListBuffer[Column]

    val dbmd = conn.getMetaData
    val pkrs = dbmd.getPrimaryKeys(db, null, table)
    val rsmd = pkrs.getMetaData

    while (pkrs.next()) {
      val primaryKeyColumn = pkrs.getString(4)

      val crs = dbmd.getColumns(db, null, table, primaryKeyColumn)
      if (crs.next()) {
        val dataType = crs.getInt(5)
        pks += Column(primaryKeyColumn, dataType)
      }
      crs.close()
    }
    pkrs.close()

    pks
  }

  /**
    * 获取列统计信息
    *
    * @param conn 数据库连接
    * @param db 数据库名
    * @param table 表名
    *
    * @return 列统计信息
    * */
  def getColumnStats(conn: Connection, db: String, table: String, column: String): (Long, Long, Long) = {
    val stat = conn.createStatement()

    val rs = stat.executeQuery(
      s"""
         |select
         |  min(${column}) as lowerbound,
         |  max(${column}) as upperbound,
         |  count(1) as count
         |from ${db}.${table}
    """.stripMargin)
    rs.next()

    val lowerBound = rs.getLong(1)
    val upperBound = rs.getLong(2)
    val count = rs.getLong(3)

    rs.close()
    stat.close()

    (lowerBound, upperBound, count)
  }

}
