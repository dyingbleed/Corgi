package com.dyingbleed.corgi.ods.core

import com.dyingbleed.corgi.ods.bean.Measure
import com.google.inject.Inject
import javax.inject.Named
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPut
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair

import scala.collection.JavaConverters._

/**
  * Created by 李震 on 2018/3/1.
  */
private[ods] class Rpc @Inject()(@Named("appName") appName: String, @Named("apiServer") apiServer: String){

  def saveMeasure(measure: Measure): Unit = {
    val httpClient = HttpClients.createDefault()

    val httpPut = new HttpPut("http://" + apiServer + "/api/v1/measure")
    val params = List(
      new BasicNameValuePair("name", measure.name),
      new BasicNameValuePair("submissionTime", measure.submissionTime.toString(Constants.DATETIME_FORMAT)),
      new BasicNameValuePair("completionTime", measure.completionTime.toString(Constants.DATETIME_FORMAT)),
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
