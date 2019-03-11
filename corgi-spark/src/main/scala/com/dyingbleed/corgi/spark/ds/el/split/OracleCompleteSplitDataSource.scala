package com.dyingbleed.corgi.spark.ds.el.split

import com.dyingbleed.corgi.spark.core.Constants
import com.dyingbleed.corgi.spark.core.ODSMode._
import org.apache.spark.sql.DataFrame
import org.joda.time.{Days, LocalDate}

/**
  * Created by 李震 on 2019/1/8.
  */
class OracleCompleteSplitDataSource extends CompleteSplitDataSource {

  override protected def loadPKRangeSplitDF: DataFrame = {
    val sql = conf.mode match {
      case COMPLETE => {
        s"""
           |(SELECT
           |	${tableMeta.toSelectExpr(tableMeta.columns)}
           |FROM ${tableMeta.db}.${tableMeta.table}
           |) t
          """.stripMargin
      }
      case UPDATE | APPEND => {
        val normalColumns = tableMeta.columns.filter(c => !c.name.equals(tableMeta.tsColumnName.get))

        s"""
           |(SELECT
           |	s.*
           |FROM (
           |  SELECT
           |    ${tableMeta.toSelectExpr(normalColumns)},
           |    NVL(${tableMeta.tsColumnName.get}, TO_TIMESTAMP('${tableMeta.tsDefaultVal.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')) AS ${tableMeta.tsColumnName.get}
           |  FROM ${tableMeta.db}.${tableMeta.table}
           |) s
           |WHERE s.${tableMeta.tsColumnName.get} < TO_TIMESTAMP('${executeDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
           |) t
          """.stripMargin
      }
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

      val sql = conf.mode match {
        case COMPLETE => {
          s"""
             |(SELECT
             |	${tableMeta.toSelectExpr(tableMeta.columns)}
             |FROM ${tableMeta.db}.${tableMeta.table}
             |WHERE MOD(ORA_HASH($hashExpr), ${Constants.DEFAULT_PARALLEL}) = $mod
             |) t
          """.stripMargin
        }
        case UPDATE | APPEND => {
          val normalColumns = tableMeta.columns.filter(c => !c.name.equals(tableMeta.tsColumnName.get))

          s"""
             |(SELECT
             |	s.*
             |FROM (
             |  SELECT
             |    ${tableMeta.toSelectExpr(normalColumns)},
             |    NVL(${tableMeta.tsColumnName.get}, TO_TIMESTAMP('${tableMeta.tsDefaultVal.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')) AS ${tableMeta.tsColumnName.get}
             |  FROM ${tableMeta.db}.${tableMeta.table}
             |) s
             |WHERE s.${tableMeta.tsColumnName.get} < TO_TIMESTAMP('${executeDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
             |AND MOD(ORA_HASH($hashExpr), ${Constants.DEFAULT_PARALLEL}) = $mod
             |) t
          """.stripMargin
        }
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
      val partitionColumnName = conf.partitionColumns(1)

      for (v <- tableMeta.distinct(partitionColumnName)) {
        val normalColumns = tableMeta.columns.filter(c => !c.name.equals(tableMeta.tsColumnName.get))

        val sql =
          s"""
             |(SELECT
             |  ${tableMeta.toSelectExpr(normalColumns)},
             |  NVL(${tableMeta.tsColumnName.get}, TO_TIMESTAMP('${tableMeta.tsDefaultVal.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')) AS ${tableMeta.tsColumnName.get}
             |FROM ${tableMeta.db}.${tableMeta.table}
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
    } else if (conf.mode == COMPLETE && conf.partitionColumns.length > 1) {
      val partitionColumnName = conf.partitionColumns(1)

      for (v <- tableMeta.distinct(partitionColumnName)) {
        val sql =
          s"""
             |(SELECT
             |  ${tableMeta.toSelectExpr(tableMeta.columns)}
             |FROM ${tableMeta.db}.${tableMeta.table}
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
    } else {
      throw new RuntimeException("不支持按分区字段分片")
    }

    unionDF
  }

}
