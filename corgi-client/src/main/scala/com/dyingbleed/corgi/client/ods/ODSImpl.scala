package com.dyingbleed.corgi.client.ods

import java.sql.Connection
import java.util.concurrent.Executors

import com.dyingbleed.corgi.client.rpc.{ConfService, RunService}
import com.dyingbleed.corgi.client.util.DataFrameUtil
import com.dyingbleed.corgi.core.constant.Mode
import com.dyingbleed.corgi.core.util.JDBCUtil
import com.dyingbleed.corgi.core.util.JDBCUtil.WithConnection
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDate
import retrofit2.Retrofit

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by 李震 on 2019-05-05.
  */
private[client] class ODSImpl (url: String, spark: SparkSession) extends ODS {

  /*
   * Private Field
   * */

  implicit private[this] val context = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  private[this] val retrofit = new Retrofit.Builder().baseUrl(url).build()

  private[this] val confService = this.retrofit.create(classOf[ConfService])

  private[this] val runService = this.retrofit.create(classOf[RunService])

  /*
   * Public Method
   * */

  override def run(name: String): Future[Unit] = {
    Future {
      runInternal(name)
    }
  }

  override def data(name: String): DataFrame = {
    assert(name != null && !name.isEmpty, "job name can not be null.")
    val odsTask = this.confService.getODSTaskConf(name)

    val df = spark.table(s"${odsTask.getSinkDb}.${odsTask.getSinkTable}")
    Mode.valueOf(odsTask.getMode) match {
      case Mode.APPEND => df
      case Mode.COMPLETE => df.filter(s"ods_date = '${LocalDate.now().toString("yyyy-MM-dd")}'")
      case Mode.UPDATE => {
        val pk = new mutable.LinkedHashSet[String]

        JDBCUtil.withAutoClose(odsTask.getDatasourceUrl, odsTask.getDatasourceUsername, odsTask.getDatasourcePassword, new WithConnection {

          override def withConnection(conn: Connection): Unit = {
            for (c <- JDBCUtil.getPrimaryKey(conn, odsTask.getSourceDb, odsTask.getSourceTable).toSeq) {
              pk += c.getName
            }
          }

        })

        assert(pk.nonEmpty, "source table must have primary key.")
        DataFrameUtil.getNewestDF(df, pk.toSeq, odsTask.getTimeColumn)
      }
    }
  }

  /*
   * Private Method
   * */

  private[this] def runInternal(name: String): Unit = {
    assert(name != null && !name.isEmpty, "job name can not be null.")
    this.runService.runODSTask(name)
  }
}
