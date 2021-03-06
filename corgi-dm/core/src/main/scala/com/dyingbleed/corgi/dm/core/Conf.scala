package com.dyingbleed.corgi.dm.core

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.dyingbleed.corgi.core.bean.DMTask
import com.dyingbleed.corgi.core.constant.{DBMSVendor, Mode}
import com.google.common.base.Charsets
import com.google.common.base.Preconditions.checkNotNull
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import scala.collection.JavaConversions._

/**
  * Created by 李震 on 2019/3/12.
  */
object Conf {

  def apply(args: Array[String]): Conf = {
    val conf = new Conf(args)
    conf.init()
    conf
  }


  lazy val debug: Boolean = "true".equals(System.getenv("CORGI_DEBUG"))

}

class Conf private[Conf] (args: Array[String]) {

  /*
   * 参数
   * */

  private[this] var _appName: String = _

  /*
   * Classpath 配置
   * */

  private[this] var _apiServer: String = _

  /*
   * API Server 配置
   * */

  private[this] var _dmTask: DMTask = _


  /*
   * 私有方法
   * */

  private[this] def initArgsConf(): Unit = {
    assert(args.length >= 1)
    _appName = args.last
  }

  private[this] def initClasspathConf(): Unit = {
    val properties = new Properties()
    val path = if (Conf.debug) "spark-test.properties" else "spark.properties"
    val propertiesIn = classOf[Conf].getClassLoader.getResourceAsStream(path)
    properties.load(propertiesIn)
    propertiesIn.close()

    val apiServer = properties.getProperty("api.server")
    checkNotNull(apiServer)
    _apiServer = apiServer
  }

  private[this] def initRemoteConf(): Unit = {
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet("http://" + _apiServer + "/api/v1/conf/dm?name=" + _appName)
    val httpResponse = httpClient.execute(httpGet)

    val statusCode = httpResponse.getStatusLine.getStatusCode
    if (statusCode >= 200 && statusCode < 300) {
      val entity = httpResponse.getEntity
      val content = EntityUtils.toString(entity, Charsets.UTF_8)
      httpClient.close()

      _dmTask = JSON.parseObject(content, classOf[DMTask])
    } else {
      httpClient.close()
      throw new RuntimeException("调用批处理任务接口返回 " + statusCode)
    }
  }

  private[Conf] def init(): Unit = {
    initArgsConf()
    initClasspathConf()
    initRemoteConf()
  }

  /*
   * 公共方法
   * */


  def appName: String = _appName

  def sourceDB: String = _dmTask.getSourceDB

  def sourceTable: String = _dmTask.getSourceTable

  def whereExp: Option[String] = Option(_dmTask.getWhereExp)

  def dayOffset: Option[Integer] = Option(_dmTask.getDayOffset)

  def mode: Mode = Mode.valueOf(_dmTask.getMode)

  def url: String = _dmTask.getDatasourceUrl

  def username: String = _dmTask.getDatasourceUsername

  def password: String = _dmTask.getDatasourcePassword

  def sinkDB: String = _dmTask.getSinkDB

  def sinkTable: String = _dmTask.getSinkTable

  def pks: Option[Seq[String]] = {
    if (null == _dmTask.getPks) {
      None
    } else {
      Option(JSON.parseArray(_dmTask.getPks, classOf[String]))
    }
  }

  def sinkVendor: DBMSVendor = {
    if (_dmTask.getDatasourceUrl.startsWith("jdbc:mysql")) {
      DBMSVendor.MYSQL
    } else if (_dmTask.getDatasourceUrl.startsWith("jdbc:oracle:thin")) {
      DBMSVendor.ORACLE
    } else {
      throw new IllegalArgumentException
    }
  }

}
