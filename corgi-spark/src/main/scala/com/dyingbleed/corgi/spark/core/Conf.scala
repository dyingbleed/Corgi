package com.dyingbleed.corgi.spark.core

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.common.base.Charsets
import com.google.common.base.Preconditions.checkNotNull
import org.apache.commons.cli.{GnuParser, Options}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.joda.time.LocalTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by 李震 on 2018/3/1.
  */
private[spark] final class Conf private (args: Array[String]) {

  /* ***************
   * Classpath 配置 *
   * ***************/
  
  private[this] var _apiServer: String = _

  private[this] var _hiveMetastoreUris: String = _

  /**
    * Corgi Web 服务地址
    * */
  def apiServer: String = _apiServer

  /**
    * Hive 元数据服务地址
    * */
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
  
  private[this] var _executeTime: Option[LocalTime] = _

  /**
    * 应用名
    * */
  def appName: String = _appName

  /**
    * 是否忽略历史
    * */
  def ignoreHistory: Boolean = _ignoreHistory

  /**
    * 分区字段
    * */
  def partitionColumns: Array[String] = _partitionColumns

  /**
    * 自定义执行时间
    * */
  def executeTime: Option[LocalTime] = _executeTime

  /* *****
   * 方法 *
   * *****/

  private def init(): Conf = {
    initArgsConf()
    initClasspathJobConf()
    initRemoteAppConf()

    this
  }

  private def initArgsConf(): Unit = {
    val options = new Options

    // 忽略历史数据
    options.addOption(Constants.CONF_IGNORE_HISTORY_SHORT, Constants.CONF_EXECUTE_TIME, false, "Ignore history data")
    // 分区字段
    options.addOption(Constants.CONF_PARTITION_COLUMNS_SHORT, Constants.CONF_PARTITION_COLUMNS, true, "Hive table partition columns")
    // 执行时间
    options.addOption(Constants.CONF_EXECUTE_TIME_SHORT, Constants.CONF_EXECUTE_TIME, true, "Customize execute time")

    val optionParser = new GnuParser
    val commandLine = optionParser.parse(options, args)

    _appName = commandLine.getArgs.last

    _ignoreHistory = commandLine.hasOption(Constants.CONF_IGNORE_HISTORY_SHORT) || commandLine.hasOption(Constants.CONF_IGNORE_HISTORY)

    _partitionColumns = {
      if (commandLine.hasOption(Constants.CONF_PARTITION_COLUMNS_SHORT) || commandLine.hasOption(Constants.CONF_PARTITION_COLUMNS)) {
        Array(Constants.DATE_PARTITION) ++ commandLine.getOptionValue(Constants.CONF_PARTITION_COLUMNS, "").split(",")
      } else {
        Array(Constants.DATE_PARTITION)
      }
    }
    
    _executeTime = {
      if (commandLine.hasOption(Constants.CONF_EXECUTE_TIME_SHORT) || commandLine.hasOption(Constants.CONF_EXECUTE_TIME)) {
        Option(LocalTime.parse(commandLine.getOptionValue(Constants.CONF_EXECUTE_TIME), DateTimeFormat.forPattern(Constants.TIME_FORMAT)))
      } else {
        None
      }
    }
  }

  private def initClasspathJobConf(): Unit = {
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

  private def initRemoteAppConf(): Unit = {
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

object Conf {

  def apply(args: Array[String]): Conf = {
    val conf = new Conf(args)
    conf.init()
    conf
  }

}
