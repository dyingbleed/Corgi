package com.dyingbleed.corgi.spark.core

import scala.collection.JavaConverters._

import com.alibaba.fastjson.JSON
import com.google.common.base.Charsets
import com.google.inject.Inject
import javax.inject.Named
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.joda.time.LocalDateTime

/**
  * Created by 李震 on 2018/3/1.
  */
class Metadata @Inject()(@Named("appName") appName: String, @Named("apiServer") apiServer: String){

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
