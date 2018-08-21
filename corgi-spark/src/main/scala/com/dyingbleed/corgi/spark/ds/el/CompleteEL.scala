package com.dyingbleed.corgi.spark.ds.el

import com.dyingbleed.corgi.spark.core.Conf
import com.dyingbleed.corgi.spark.ds.{DataSourceEL, DataSourceUtils}
import com.google.inject.Inject
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.LocalDate

/**
  * Created by 李震 on 2018/6/26.
  */
private[spark] class CompleteEL extends DataSourceEL {

  @Inject
  var spark: SparkSession = _

  @Inject
  var conf: Conf = _

  /**
    * 加载数据源
    *
    * @return
    **/
  override def loadSourceDF: DataFrame = {
    val reader = spark.read
      .format("jdbc")
      .option("url", conf.sourceDbUrl)
      .option("dbtable", conf.sourceTable)
      .option("user", conf.sourceDbUser)
      .option("password", conf.sourceDbPassword)

      if (conf.sourceDbUrl.startsWith("jdbc:mysql")) {
        reader.option("driver", "com.mysql.jdbc.Driver")
      } else if(conf.sourceDbUrl.startsWith("jdbc:oracle:thin")) {
        reader.option("driver", "oracle.jdbc.OracleDriver")
      } else {
        throw new RuntimeException("不支持的数据源")
      }

      reader.load()
  }

  /**
    * 持久化数据源
    *
    * @return
    **/
  override def persistSinkDF(df: DataFrame): Unit = {
    // 创建表
    val schema = df.schema.add("ods_date", StringType)
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema).createOrReplaceTempView("t")
    spark.sql(
      s"""
         |create table if not exists ${conf.sinkDb}.${conf.sinkTable}
         |using PARQUET
         |partitioned by (ods_date)
         |as select * from t
          """.stripMargin
    )

    // 增加分区
    spark.sql(
      s"""
         |alter table ${conf.sinkDb}.${conf.sinkTable}
         |add if not exists partition (ods_date='${LocalDate.now().toString("yyyy-MM-dd")}')
          """.stripMargin)

    // 插入数据
    // 静态分区
    DataSourceUtils.forceInsertOverwriteTablePartition(df, conf.sinkDb, conf.sinkTable, LocalDate.now())
  }

}
