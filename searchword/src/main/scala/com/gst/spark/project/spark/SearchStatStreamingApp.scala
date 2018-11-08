package com.gst.spark.project.spark

import java.sql.DriverManager

import com.gst.spark.project.dao.SearchWordCountDAO
import com.gst.spark.project.domain.{SearchWordCount, SearchWordLog}
import com.gst.spark.project.utils.RegJson.regJson
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import com.gst.spark.project.utils.ConnectionPool
import scala.util.parsing.json.JSON

/**
  *  微信首页搜索关键字实时统计
  *  @author huican.chen
  */
object SearchStatStreamingApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Usage: SearchStatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args
    val sparkConf  = new SparkConf().setAppName("SearchStatStreamingApp") //.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val message = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    val logs = message.map(_._2)
    val cleanData = logs.map(line => {
      val start_idx = line.lastIndexOf("message")
      val end_idx = line.indexOf("time=")
      val lines = line.substring(start_idx+10, end_idx-1)
      //println(message)
      val infos = lines.split(",").toList.map(x=>x.split("=",2)).map(x=>Tuple2(x(0), x(1))).toMap
      val wordsList = infos.map(_._2).toList
      val city_id = wordsList(1).toString
      val keywords = wordsList(2).toString
      //val time = wordsList(3).toString.replaceAll("\"","")
      SearchWordLog(city_id, keywords)
    })

    //1、 存入Hbase  暂时废弃
//    cleanData.map(x => {
//      (x.cityid.toString + "_" + x.keywords, x.cityid, x.keywords, 1)
//    }).foreachRDD(rdd => {
//      rdd.foreachPartition(partitionRecords => {
//        val list = new ListBuffer[SearchWordCount]
//
//        partitionRecords.foreach(pair => {
//          list.append(SearchWordCount((pair._1).toString, (pair._2).toString, (pair._3).toString, (pair._4).toString))
//        })
//
//        SearchWordCountDAO.save(list)
//      })
//    })

    //2、存入MySQL
    cleanData.map(x => {
      // HBase rowkey设计： cityid_keywords
      (x.cityid.toString + "_" + x.keywords, x.cityid, x.keywords, 1)
    }).foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        rdd.foreachPartition(partitionRecords => {
          val conn = ConnectionPool.getConnection
          val stmt = conn.createStatement
          try {
            conn.setAutoCommit(false)
            partitionRecords.foreach(record => {
              val clinic_count = 1
              val sqls = "insert into F_SearchWords_V(Row_Key," +
                "City_Id," +
                "Key_Word," +
                "RanKing) " +
                "values ('" + record._1 + "','" + record._2 + "','"+ record._3 + "'," + record._4 +
                ")" +  "ON DUPLICATE KEY UPDATE RanKing=RanKing+1"
              stmt.addBatch(sqls)
              //stmt.executeUpdate(sqls)
            })
            stmt.executeBatch
            conn.commit()
          }catch {
            case e: Exception =>
              println("插入数据失败!")
          }
          ConnectionPool.returnConnection(conn)
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
