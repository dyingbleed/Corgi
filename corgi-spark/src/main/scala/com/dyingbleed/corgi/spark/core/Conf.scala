package com.dyingbleed.corgi.spark.core

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.common.base.Charsets
import com.google.common.base.Preconditions.checkNotNull
import org.apache.commons.cli.{GnuParser, Options}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
  * Created by 李震 on 2018/3/1.
  */
private[spark] class Conf(args: Array[String]) {

  /* ***************
   * Classpath 配置 *
   * ***************/
  private[this] var _apiServer: String = _

  private[this] var _hiveMetastoreUris: String = _

  def apiServer: String = _apiServer

  def hiveMetastoreUris: String = _hiveMetastoreUris

  /* *********
   * 远端配置 *
   * *********/

  private[this] var _batchTaskApiJSON: JSONObject = _

  /**
    * 模式：COMPLETE UPDATE APPEND
    *
    * - COMPLETE 全量
    * - UPDATE 增量更新
    * - APPEND 增量追加
    *
    * */
  def mode: ODSMode = ODSMode.valueOf(_batchTaskApiJSON.getString("mode"))

  /**
    * 时间戳字段名
    * */
  def sourceTimeColumn: String = {
    val timeColumn = _batchTaskApiJSON.getString("timeColumn")

    if (timeColumn.trim.length == 0) null
    else timeColumn
  }

  /**
    * Source 数据库连接 URL
    * */
  def sourceDbUrl: String = _batchTaskApiJSON.getString("dataSourceUrl")

  /**
    * Source 数据库名
    * */
  def sourceDb: String = _batchTaskApiJSON.getString("sourceDb")

  /**
    * Source 表名
    * */
  def sourceTable: String = _batchTaskApiJSON.getString("sourceTable")

  /**
    * Source 数据库连接用户名
    * */
  def sourceDbUser: String = _batchTaskApiJSON.getString("dataSourceUsername")

  /**
    * Source 数据库连接密码
    * */
  def sourceDbPassword: String = _batchTaskApiJSON.getString("dataSourcePassword")

  /**
    * Sink 数据库名
    * */
  def sinkDb: String = _batchTaskApiJSON.getString("sinkDb")

  /**
    * Sink 表名
    * */
  def sinkTable: String = _batchTaskApiJSON.getString("sinkTable")

  /* *********
   * 本地配置 *
   * *********/
  private[this] var _appName: String = _

  private[this] var _ignoreHistory: Boolean = _

  private[this] var _partitionColumns: Array[String] = _

  def appName: String = _appName

  def ignoreHistory: Boolean = _ignoreHistory

  def partitionColumns: Array[String] = _partitionColumns

  /* *********
   * 公共方法 *
   * *********/

  def init(): Conf = {
    loadArgsConf()
    loadClasspathJobConf()
    loadRemoteAppConf()

    this
  }

  private def loadArgsConf(): Unit = {
    val options = new Options

    // 忽略历史数据
    options.addOption("ig", false, "Ignore history data")
    // 分区字段
    options.addOption("p", true, "Hive table partition columns")

    val optionParser = new GnuParser
    val commandLine = optionParser.parse(options, args)

    _appName = commandLine.getArgs.last

    _ignoreHistory = commandLine.hasOption("ig")

    _partitionColumns = {
      if (commandLine.hasOption("p")) {
        Array("ods_date") ++ commandLine.getOptionValue("p", "").split(",")
      } else {
        Array("ods_date")
      }
    }
  }

  private def loadClasspathJobConf(): Unit = {
    val properties = new Properties()
    val propertiesIn = classOf[Conf].getClassLoader.getResourceAsStream("spark.properties")
    properties.load(propertiesIn)
    propertiesIn.close()

    val apiServer = properties.getProperty("api.server")
    checkNotNull(apiServer)
    _apiServer = apiServer

    val hiveMetastoreUris = properties.getProperty("hive.metastore.uris")
    checkNotNull(hiveMetastoreUris)
    _hiveMetastoreUris = hiveMetastoreUris
  }

  private def loadRemoteAppConf(): Unit = {
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet("http://" + _apiServer + "/api/conf?name=" + _appName)
    val httpResponse = httpClient.execute(httpGet)

    val statusCode = httpResponse.getStatusLine.getStatusCode
    if (statusCode >= 200 && statusCode < 300) {
      val entity = httpResponse.getEntity
      val content = EntityUtils.toString(entity, Charsets.UTF_8)
      httpClient.close()

      _batchTaskApiJSON = JSON.parseObject(content)
    } else {
      httpClient.close()
      throw new RuntimeException("调用批处理任务接口返回 " + statusCode)
    }
  }
}
