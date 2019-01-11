package com.dyingbleed.corgi.spark.ds.el.split

import com.dyingbleed.corgi.spark.core.Constants
import com.dyingbleed.corgi.spark.core.ODSMode._
import org.apache.spark.sql.DataFrame
import org.joda.time.{Days, LocalDate}

/**
  * Created by 李震 on 2019/1/8.
  */
private[spark] class MySQLCompleteSplitDataSource extends CompleteSplitDataSource {

  override protected def loadPKRangeSplitDF: DataFrame = {
    val sql = if (tableMeta.tsColumnName.isEmpty) s"${tableMeta.db}.${tableMeta.table}" else {
      val selectExp = tableMeta.columns
        .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
        .map(c => c.name)
        .mkString(",")

      s"""
         |(SELECT
         |  *
         |FROM (
         |  SELECT
         |    $selectExp,
         |    IFNULL(${tableMeta.tsColumnName.get}, TIMESTAMP('${tableMeta.tsDefaultVal.toString(Constants.DATETIME_FORMAT)}')) AS ${tableMeta.tsColumnName.get}
         |  FROM ${tableMeta.db}.${tableMeta.table}
         |) s
         |WHERE ${tableMeta.tsColumnName.get} < TIMESTAMP('${executeDateTime.toString(Constants.DATETIME_FORMAT)}')
         |) t
          """.stripMargin
    }

    val pkStats = tableMeta.stats(tableMeta.pk.last.name)
    jdbcDF(sql, tableMeta.pk.last.name, cast2Long(pkStats.max), cast2Long(pkStats.min), Math.min((pkStats.cardinality / 10000) + 1, Constants.DEFAULT_PARALLEL))
  }

  override protected def loadPKHashSplitDF: DataFrame = {
    var unionDF: DataFrame = null

    for (mod <- 0l until Constants.DEFAULT_PARALLEL) {
      val hashExpr = if (tableMeta.pk.size > 1) {
        tableMeta.pk.foldLeft("''")((x, y) => s"CONCAT($x, ${y.name})")
      } else {
        tableMeta.pk.last.name
      }

      val sql = if (tableMeta.tsColumnName.isEmpty) {
        s"""
           |(SELECT
           |  *
           |FROM ${tableMeta.db}.${tableMeta.table}
           |WHERE MOD(CONV(MD5($hashExpr), 16, 10), ${Constants.DEFAULT_PARALLEL}) = $mod
           |) t
          """.stripMargin
      } else {
        val selectExp = tableMeta.columns
          .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
          .map(c => c.name)
          .mkString(",")

        s"""
           |(SELECT
           |  *
           |FROM (
           |  SELECT
           |    $selectExp,
           |    IFNULL(${tableMeta.tsColumnName}, TIMESTAMP('${tableMeta.tsDefaultVal.toString(Constants.DATETIME_FORMAT)}')) AS ${tableMeta.tsColumnName}
           |  FROM ${tableMeta.db}.${tableMeta.table}
           |) s
           |WHERE ${tableMeta.tsColumnName.get} < TIMESTAMP('${executeDateTime.toString(Constants.DATETIME_FORMAT)}')
           |AND MOD(CONV(MD5($hashExpr), 16, 10), ${Constants.DEFAULT_PARALLEL}) = $mod
           |) t
          """.stripMargin
      }

      val df = jdbcDF(sql)
      if (unionDF == null) {
        unionDF = df
      } else {
        unionDF = unionDF.union(df)
      }
    }

    unionDF
  }

  override protected def loadPartitionSplitDF: DataFrame = {
    var unionDF: DataFrame = null

    if (conf.mode == UPDATE || conf.mode == APPEND) {
      val days = Days.daysBetween(tableMeta.tsDefaultVal, LocalDate.now()).getDays
      for (d <- 0 to days) {
        val selectExp = tableMeta.columns
          .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
          .map(c => c.name)
          .mkString(",")

        val sql =
          s"""
             |(SELECT
             |	s.*
             |FROM (
             |  SELECT
             |    $selectExp,
             |    NVL(${tableMeta.tsColumnName.get}, TO_DATE('${tableMeta.tsDefaultVal.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')) AS ${tableMeta.tsColumnName.get}
             |  FROM ${tableMeta.db}.${tableMeta.table}
             |) s
             |WHERE s.${tableMeta.tsColumnName.get} >= TO_DATE('${tableMeta.tsDefaultVal.plusDays(d).toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
             |AND s.${tableMeta.tsColumnName.get} < TO_DATE('${tableMeta.tsDefaultVal.plusDays(d + 1).toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
             |) t
        """.stripMargin

        val df = jdbcDF(sql)
        if (unionDF == null) {
          unionDF = df
        } else {
          unionDF = unionDF.union(df)
        }
      }
    } else if (conf.mode == COMPLETE && conf.partitionColumns.length > 1) {
      val partitionColumnName = conf.partitionColumns(1)

      for (v <- tableMeta.distinct(partitionColumnName)) {
        val selectExp = tableMeta.columns
          .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
          .map(c => c.name)
          .mkString(",")

        val sql =
          s"""
            |(SELECT
            |  *
            |FROM (
            |  SELECT
            |    $selectExp,
            |    NVL(${tableMeta.tsColumnName.get}, TO_DATE('${tableMeta.tsDefaultVal.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')) AS ${tableMeta.tsColumnName.get}
            |  FROM ${tableMeta.db}.${tableMeta.table}
            |) s
            |WHERE $partitionColumnName = ${toSQLExpr(v)}
            |) t
          """.stripMargin

        val df = jdbcDF(sql)
        if (unionDF == null) {
          unionDF = df
        } else {
          unionDF = unionDF.union(df)
        }
      }
    }

    unionDF
  }

}
