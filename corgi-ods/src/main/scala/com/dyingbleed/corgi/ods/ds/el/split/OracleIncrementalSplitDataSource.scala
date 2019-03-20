package com.dyingbleed.corgi.ods.ds.el.split

import com.dyingbleed.corgi.ods.core.Constants
import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2019/1/8.
  */
private[ods] class OracleIncrementalSplitDataSource extends IncrementalSplitDataSource {

  override protected def loadPKRangeSplitDF: DataFrame = {
    val sql = s"""
                 |(SELECT
                 |  ${tableMeta.toSelectExpr(tableMeta.columns)}
                 |FROM ${tableMeta.db}.${tableMeta.table}
                 |WHERE ${tableMeta.tsColumnName.get} > TO_TIMESTAMP('${lastExecuteDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
                 |AND ${tableMeta.tsColumnName.get} < TO_TIMESTAMP('${executeDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
                 |) t
              """.stripMargin

    val pkStats = tableMeta.stats(tableMeta.pk.last.getName)
    jdbcDF(sql, tableMeta.pk.last.getName, cast2Long(pkStats.max), cast2Long(pkStats.min), Math.min((pkStats.cardinality / 10000) + 1, Constants.DEFAULT_PARALLEL))
  }

  override protected def loadPKHashSplitDF: DataFrame = {
    var unionDF: DataFrame = null

    for (mod <- 0l until Constants.DEFAULT_PARALLEL) {
      val hashExpr = if (tableMeta.pk.size > 1) {
        tableMeta.pk.foldLeft("''")((x, y) => s"CONCAT($x, ${y.getName})")
      } else {
        tableMeta.pk.last.getName
      }

      val sql = s"""
                   |(SELECT
                   |	${tableMeta.toSelectExpr(tableMeta.columns)}
                   |FROM ${tableMeta.db}.${tableMeta.table}
                   |WHERE MOD(ORA_HASH($hashExpr), ${Constants.DEFAULT_PARALLEL}) = $mod
                   |AND ${tableMeta.tsColumnName.get} > TO_TIMESTAMP('${lastExecuteDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
                   |AND ${tableMeta.tsColumnName.get} < TO_TIMESTAMP('${executeDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
                   |) t
                """.stripMargin

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

    val partitionColumnName = conf.partitionColumns(1)

    for (v <- tableMeta.distinct(partitionColumnName, lastExecuteDateTime, executeDateTime)) {
      val sql = s"""
                   |(SELECT
                   |  ${tableMeta.toSelectExpr(tableMeta.columns)}
                   |FROM ${tableMeta.db}.${tableMeta.table}
                   |WHERE ${tableMeta.tsColumnName.get} > TO_TIMESTAMP('${lastExecuteDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
                   |AND ${tableMeta.tsColumnName.get} < TO_TIMESTAMP('${executeDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
                   |AND $partitionColumnName = ${toSQLExpr(v)}
                   |) t
                  """.stripMargin

      val df = jdbcDF(sql)
      if (unionDF == null) {
        unionDF = df
      } else {
        unionDF = unionDF.union(df)
      }
    }

    unionDF
  }
}
