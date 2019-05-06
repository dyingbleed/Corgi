package com.dyingbleed.corgi.client.dm

import org.apache.spark.sql.DataFrame

import scala.concurrent.Future

/**
  * Created by 李震 on 2019-05-05.
  */
trait DM {

  def run(name: String): Future[Unit]

  def data(name: String): DataFrame

}
