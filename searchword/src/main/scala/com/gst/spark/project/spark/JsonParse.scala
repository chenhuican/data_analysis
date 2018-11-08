package com.gst.spark.project.spark
import com.gst.spark.project.domain.SearchWordLog

import scala.util.parsing.json.JSON

object JsonParse {
  def main(args: Array[String]): Unit = {
    def regJson(json:Option[Any]) = json match {
      case Some(map: Map[String, Any]) => map
      //case other => println("Error jsonStr")
    }

    val str2 = """{"@version":"1","host":"120.76.26.54","headers":{"http_cache_control":"no-cache","http_version":"HTTP/1.0","http_connection":"close","http_postman_token":"c5a46d85-b73c-f2ed-8820-adeb19f7d8f6","request_method":"POST","http_host":"data-api.gstzy.cn","request_uri":"/log","http_user_agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36","http_accept_language":"zh-CN,zh;q=0.9","http_accept":"*/*","content_type":"application/json","http_accept_encoding":"gzip, deflate","http_x_forwarded_for":"113.119.56.106","request_path":"/log","http_x_real_ip":"113.119.56.106","http_origin":"chrome-extension://fhbjgbiflinjbdggehcddcbncdddomop","content_length":"92"},"message":"type=searchwords,city_id=755,keyword=竹子林门店地址11,time=2018/10/22","@timestamp":"2018-10-22T01:30:33.217Z"}"""

    val str = "{\"host\":\"td_test\",\"ts\":1486979192345,\"device\":{\"tid\":\"a123456\",\"os\":\"android\",\"sdk\":\"1.0.3\"},\"time\":1501469230058}"
    val jsonS = JSON.parseFull(str2)
    val first = regJson(jsonS)

    // 获取一级key
    println(first.get("message"))
    // 获取二级key
   // val dev = first.get("device")
   // println(dev)
   // val sec = regJson(dev)
    val lines = first.get("message").toString.replace("Some(","").replace(")","")
    println(lines)

    val infos = lines.split(",").toList.map(x=>x.split("=",2)).map(x=>Tuple2(x(0),x(1))).toMap
    val wordsList =infos.map(_._2).toList
    val city_id   = wordsList(1).toString
    val keywords  = wordsList(2).toString
    val time = wordsList(3).toString
    SearchWordLog(city_id, keywords)

    println(wordsList)

    // 将获取的headers转为Map
    val attrObj = first.get("headers").get.asInstanceOf[Map[String, String]]
    println(attrObj)

  }

}
