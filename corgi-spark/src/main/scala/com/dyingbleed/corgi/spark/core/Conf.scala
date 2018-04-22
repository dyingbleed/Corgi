package com.dyingbleed.corgi.spark.core

import com.alibaba.fastjson.JSON
import com.google.inject.Inject
import com.google.inject.name.Named
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by 李震 on 2018/3/1.
  */
class Conf @Inject()(@Named("appName") appName: String) {

  private val jobConf = JSON.parseObject(loadHDFS(s"${Constants.JOB_CONF_BASE_PATH}/${appName}.json"))
  
  private val sourceConf = jobConf.getJSONObject("source")
  private val sinkConf = jobConf.getJSONObject("sink")
  
  private val dbConf = JSON.parseObject(loadHDFS(s"${Constants.DB_CONF_BASE_PATH}/${sourceConf.getString("db")}.json"))

  private val cacheConf = JSON.parseObject(loadHDFS(Constants.CACHE_CONF_BASE_PATH))

  private def loadHDFS(filePath: String): String = {
    val conf = new Configuration()
    val path = new Path(filePath)
    val fs = FileSystem.get(path.toUri, conf)

    val inputStream = fs.open(path)
    val content = IOUtils.toString(inputStream)
    IOUtils.closeQuietly(inputStream)

    content
  }

  def mode: ODSMode = ODSMode.valueOf(sourceConf.getString("mode"))

  def modifyTimeColumn: String = sourceConf.getString("modify_time_column")

  def dbUrl: String = dbConf.getString("url")

  def dbTable: String = sourceConf.getString("table")

  def dbUser: String = dbConf.getString("username")

  def dbPassword: String = dbConf.getString("password")

  def cacheHost: String = cacheConf.getString("host")

  def cachePort: Integer = cacheConf.getInteger("port")

  def cachePassword: String = cacheConf.getString("password")

  def hiveDB: String = sinkConf.getString("db")

  def hiveTable: String = sinkConf.getString("table")

}
