package com.dyingbleed.corgi.client.ods

import org.apache.spark.sql.DataFrame

import scala.concurrent.Future

/**
  * Created by 李震 on 2019-05-05.
  */
trait ODS {

  def run(name: String): Future[Unit]

  def data(name: String): DataFrame

}
