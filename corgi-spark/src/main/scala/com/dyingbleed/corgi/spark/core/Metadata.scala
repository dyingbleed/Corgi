package com.dyingbleed.corgi.spark.core

import javax.inject.Named

import com.google.inject.Inject
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import redis.clients.jedis.Jedis

/**
  * Created by 李震 on 2018/3/1.
  */
class Metadata @Inject()(@Named("appName") appName: String){

  @Inject
  var conf: Conf = _

  def getLastModifyDate: LocalDateTime = {
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet("http://" + apiServer + "/api/metric?name=" + appName)
    val httpResponse = httpClient.execute(httpGet)

    val statusCode = httpResponse.getStatusLine.getStatusCode
    if (statusCode >= 200 && statusCode < 300) {
      val entity = httpResponse.getEntity
      val content = EntityUtils.toString(entity, Charsets.UTF_8)
      httpClient.close()

      LocalDateTime.fromDateFields(JSON.parseObject(content).getDate("execute_time"))
    } else {
      httpClient.close()
      throw new RuntimeException("调用任务指标接口返回 " + statusCode)
    }
  }

  def saveLastModifyDate(lastModifyTime: LocalDateTime): Unit = {
    val httpClient = HttpClients.createDefault()

    val httpPost = new HttpPost("http://" + apiServer + "/api/metric")
    val params = List(
      new BasicNameValuePair("batch_task_name", appName),
      new BasicNameValuePair("execute_time", lastModifyTime.toString("yyyy-MM-dd HH:mm:ss"))
    )
    httpPost.setEntity(new UrlEncodedFormEntity(params.asJava))
    httpClient.execute(httpPost)

    httpClient.close()
  }

}
