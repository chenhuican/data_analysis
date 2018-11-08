package com.gst.spark.project.spark

import java.sql.DriverManager

import com.gst.spark.project.dao.CourseClickCountDAO
import com.gst.spark.project.domain.{ClinicLog, CourseClickCount}
import com.gst.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * nginx搜索日志分析
  *
  * @author huican.chen
  * Spark01:2181,Spark02:2181,Spark03:2181 test-groups test-topic 2
  */
object LogStatStreamingApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: LogStatStreamingApp  <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("LogStatStreamingApp").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(60))
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val message = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    //message.map(_._2).count.print()

    val logs = message.map(_._2)

    val cleanData = logs.map(line => {
      val infos = line.split(",")
      val url = infos(2).split(" ")(1)
      var courseId = 0

      if (url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }
      ClinicLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3), infos(4))
    })

    cleanData.map(x => {
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_+_).foreachRDD( rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val list = new ListBuffer[CourseClickCount]
        partitionOfRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })
        CourseClickCountDAO.save(list)
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 获取MySQL连接
    * @return
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql//hadoop.node1:3306/test","root","360gst.com")
  }


}
