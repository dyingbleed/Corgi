package com.dyingbleed.corgi.spark.util

import java.sql.{Connection, DriverManager, ResultSet}

import com.dyingbleed.corgi.spark.bean.{Column, ColumnStat}

import scala.collection.mutable.ListBuffer

/**
  * Created by 李震 on 2018/9/27.
  */
object JDBCUtils {

  def getConnection(url: String, username: String, password: String): Connection = {
    if (url.startsWith("jdbc:mysql")) {
      Class.forName("com.mysql.jdbc.Driver")
    } else if(url.startsWith("jdbc:oracle:thin")) {
      Class.forName("oracle.jdbc.OracleDriver")
    } else {
      throw new RuntimeException("不支持的数据源")
    }
    DriverManager.getConnection(url, username, password)
  }

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
    * 获取列信息
    *
    * @param conn 数据库连接
    * @param db 数据库名
    * @param table 表名
    *
    * @return 列信息
    * */
  def getColumns(conn: Connection, db: String, table: String): Seq[Column] = {
    val cs = new ListBuffer[Column]

    val metaData = conn.getMetaData
    val crs = metaData.getColumns(db, db, table, null)

    while (crs.next()) {
      val cName = crs.getString(4)
      val ctype = crs.getInt(5)
      cs += Column(cName, ctype)
    }
    crs.close()

    cs
  }

  private def getOne[T](conn: Connection, sql: String, rsHandler: ResultSet => T): T = {
    val stat = conn.createStatement()
    val rs = stat.executeQuery(sql)
    rs.next()
    val r = rsHandler(rs)
    rs.close()
    stat.close()
    r
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
  def getColumnStat(conn: Connection, db: String, table: String, column: String): ColumnStat = {
    val sql = s"""
         |select
         |  max($column) as max,
         |  min($column) as min,
         |  count(1) as count
         |from $db.$table
    """.stripMargin
    getOne(conn, sql, rs => {
      val max = rs.getObject(1)
      val min = rs.getObject(2)
      val count = rs.getLong(3)
      ColumnStat(max, min, count)
    })
  }

  /**
    * 获取最大值
    *
    * @param conn 数据库连接
    * @param db 数据库名
    * @param table 表名
    *
    * @return 表基数信息
    * */
  def getColumnMax(conn: Connection, db: String, table: String, column: String): Any = {
    val sql =
      s"""
         |select
         |  max($column) as max
         |from $db.$table
         |where $column is not null
    """.stripMargin
    getOne(conn, sql, rs => rs.getObject(1))
  }

  /**
    * 获取最小值
    *
    * @param conn 数据库连接
    * @param db 数据库名
    * @param table 表名
    *
    * @return 表基数信息
    * */
  def getColumnMin(conn: Connection, db: String, table: String, column: String): Any = {
    val sql =
      s"""
         |select
         |  min($column) as min
         |from $db.$table
         |where $column is not null
    """.stripMargin
    getOne(conn, sql, rs => rs.getObject(1))
  }

  /**
    * 获取表基数信息
    *
    * @param conn 数据库连接
    * @param db 数据库名
    * @param table 表名
    *
    * @return 表基数信息
    * */
  def getCardinality(conn: Connection, db: String, table: String): Long = {
    val sql =
      s"""
         |select
         |  count(1) as count
         |from $db.$table
    """.stripMargin
    getOne(conn, sql, rs => rs.getLong(1))
  }

}
