package com.gst.spark.project.spark
import com.gst.spark.project.utils.ConnectionPool

import scala.util.parsing.json.JSON

object JsonTestApp extends Serializable{
  def main(args: Array[String]): Unit = {
    val jsonStr = """{"@version":"1","host":"120.76.26.54","headers":{"http_cache_control":"no-cache","http_version":"HTTP/1.0","http_connection":"close","http_postman_token":"c5a46d85-b73c-f2ed-8820-adeb19f7d8f6","request_method":"POST","http_host":"data-api.gstzy.cn","request_uri":"/log","http_user_agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36","http_accept_language":"zh-CN,zh;q=0.9","http_accept":"*/*","content_type":"application/json","http_accept_encoding":"gzip, deflate","http_x_forwarded_for":"113.119.56.106","request_path":"/log","http_x_real_ip":"113.119.56.106","http_origin":"chrome-extension://fhbjgbiflinjbdggehcddcbncdddomop","content_length":"92"},"message":"type=searchwords,city_id=755,keyword=南山门店地址12,time=2018/11/06","@timestamp":"2018-10-22T01:30:33.217Z"}"""
    val jsonValue = JSON.parseFull(jsonStr)
//    val jsonObj = jsonValue match {
//      case Some(map:Map[String, String]) => map
//      case _ => println("ERROR jsonStr")
//    }
    val jsonObj = jsonValue match {
      case Some(map:Map[String, Any]) => map
      case other => println("Error jsonStr")
    }

    val i = jsonStr.lastIndexOf("message")
    val x = jsonStr.lastIndexOf("time")
    val lines = jsonStr.substring(i+10,x-2)
    val infos = lines.split(",").toList.map(x=>x.split("=",2)).map(x=>Tuple2(x(0), x(1))).toMap
    val wordsList = infos.map(_._2).toList
    val city_id = wordsList(1).toInt
    val keywords = wordsList(2).toString
    //val time = wordsList(3).toString
    val rowkey = city_id.toString + "_" + keywords
    println(city_id, keywords)

    val clinic_count = 1
    val conn = ConnectionPool.getConnection
    val stmt = conn.createStatement

    try {
      conn.setAutoCommit(false)
      for( a <- 1 until 10) {
        val sqls = "insert into F_SearchWords_V(Row_Key," +
          "City_Id," +
          "Key_Word," +
          "RanKing) " +
          "values ('" + rowkey + "','" + city_id + "','"+ keywords + "'," + clinic_count +
          ")" +  "ON DUPLICATE KEY UPDATE RanKing=RanKing+1"
        stmt.addBatch(sqls)

        //stmt.executeUpdate(sqls)
      }
    }catch {
      case e:Exception =>
        println("插入数据失败！")
    }
    stmt.executeBatch
    conn.commit
    ConnectionPool.returnConnection(conn)

  }
}