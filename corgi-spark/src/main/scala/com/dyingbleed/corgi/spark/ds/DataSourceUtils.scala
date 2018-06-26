package com.dyingbleed.corgi.spark.ds

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
  def forceInsertOverwriteTablePartition(df: DataFrame, db: String, table: String, date: LocalDate): Unit = {
    val spark = df.sparkSession
    val location = spark.sql(s"desc formatted ${db}.${table}").filter("col_name like 'Location%'").collect().last.getString(1)
    val path = s"${location}/ods_date=${date.toString("yyyy-MM-dd")}"
    df.coalesce(8).write.mode(SaveMode.Overwrite).parquet(path)
  }

}
