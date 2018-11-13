package com.dyingbleed.corgi.spark.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.{Hive, Table}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.LocalDate

/**
  * Created by 李震 on 2018/6/26.
  */
object DataSourceUtils {

  /**
    * 强制插入覆盖到表分区
    *
    * @param df
    * @param db
    * @param table
    * @param date
    *
    * */
  def forceInsertOverwriteTablePartition(df: DataFrame, db: String, table: String, date: LocalDate): Unit = withHive(hive => {
    val tableMeta = hive.getTable(db, table)
    val location = tableMeta.getDataLocation
    val partitionColumnName = getPartColName(tableMeta)

    val path = s"$location/$partitionColumnName=${date.toString("yyyy-MM-dd")}"
    df.write.mode(SaveMode.Overwrite).parquet(path)
  })

  private[this] def withHive(f: Hive => Unit): Unit = {
    val conf = new Configuration()
    conf.set("hive.metastore.uris", System.getProperty("hive.metastore.uris"))
    val hiveConf = new HiveConf(conf, classOf[HiveConf])
    val hive = Hive.get(hiveConf)

    f(hive)
  }

  private[this] def getPartColName(tableMeta: Table): String = {
    val originColName = tableMeta.getPartCols.get(0).getName
    val sparkColName = tableMeta.getParameters.get("spark.sql.sources.schema.partCol.0")

    if (sparkColName != null) sparkColName else originColName
  }

}
