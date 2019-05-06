package com.dyingbleed.corgi.client.dm

import java.util.concurrent.Executors

import com.dyingbleed.corgi.client.rpc.{ConfService, RunService}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDate
import retrofit2.Retrofit

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by 李震 on 2019-05-05.
  */
private[client] class DMImpl (url: String, spark: SparkSession) extends DM {

  /*
   * Private Field
   * */

  implicit private[this] val context = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  private[this] val retrofit = new Retrofit.Builder().baseUrl(url).build()

  private[this] val confV1Service = this.retrofit.create(classOf[ConfService])

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
    val dmTask = this.confV1Service.getDMTaskConf(name)

    val df = spark.table(s"${dmTask.getSourceDB}.${dmTask.getSourceTable}")

    val dayOffset = dmTask.getDayOffset
    val whereExp = dmTask.getWhereExp
    if (dayOffset != null && whereExp != null && !whereExp.isEmpty) {
      val date = LocalDate.now().plusDays(dayOffset)
      val filterExp = raw"date\('([^']*)'\)".r.replaceAllIn(whereExp, m => s"'${date.toString(m.group(1))}'")

      df.filter(filterExp)
    } else df
  }

  /*
   * Private Method
   * */

  private[this] def runInternal(name: String): Unit = {
    assert(name != null && !name.isEmpty, "job name can not be null.")
    this.runService.runDMTask(name)
  }

}
