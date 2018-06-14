package com.dyingbleed.corgi.spark.core

import com.alibaba.fastjson.JSON
import com.dyingbleed.corgi.spark.measure.Measure
import com.google.common.base.Charsets
import com.google.inject.Inject
import javax.inject.Named
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPut}
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.joda.time.LocalDateTime
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._

/**
  * Created by 李震 on 2018/3/1.
  */
private[spark] class Rpc @Inject()(@Named("appName") appName: String, @Named("apiServer") apiServer: String){

  def getLastExecuteTime: LocalDateTime = {
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet("http://" + apiServer + "/api/measure/execute/last?name=" + appName)
    val httpResponse = httpClient.execute(httpGet)

    val statusCode = httpResponse.getStatusLine.getStatusCode
    if (statusCode >= 200 && statusCode < 300) {
      val entity = httpResponse.getEntity
      val content = EntityUtils.toString(entity, Charsets.UTF_8)
      httpClient.close()

      LocalDateTime.parse(JSON.parseObject(content).getString("executeTime"), ISODateTimeFormat.dateTime())
    } else {
      httpClient.close()
      throw new RuntimeException("调用任务指标接口返回 " + statusCode)
    }
  }

  def saveExecuteTime(lastModifyTime: LocalDateTime): Unit = {
    val httpClient = HttpClients.createDefault()

    val httpPut = new HttpPut("http://" + apiServer + "/api/measure/execute")
    val params = List(
      new BasicNameValuePair("batchTaskName", appName),
      new BasicNameValuePair("executeTime", lastModifyTime.toString("yyyy-MM-dd HH:mm:ss"))
    )
    httpPut.setEntity(new UrlEncodedFormEntity(params.asJava))
    httpClient.execute(httpPut)

    httpClient.close()
  }

  def saveMeasure(measure: Measure): Unit = {
    val httpClient = HttpClients.createDefault()

    val httpPut = new HttpPut("http://" + apiServer + "/api/measure")
    val params = List(
      new BasicNameValuePair("name", measure.name),
      new BasicNameValuePair("submissionTime", measure.submissionTime.toString("yyyy-MM-dd HH:mm:ss")),
      new BasicNameValuePair("completionTime", measure.completionTime.toString("yyyy-MM-dd HH:mm:ss")),
      new BasicNameValuePair("elapsedSeconds", measure.elapsedSeconds.toString),
      new BasicNameValuePair("inputRows", measure.inputRows.toString),
      new BasicNameValuePair("inputData", measure.inputData.toString),
      new BasicNameValuePair("outputRows", measure.outputRows.toString),
      new BasicNameValuePair("outputData", measure.outputData.toString)
    )
    httpPut.setEntity(new UrlEncodedFormEntity(params.asJava))
    httpClient.execute(httpPut)

    httpClient.close()
  }

}
