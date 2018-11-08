package com.gst.spark.project.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * rdd -> mysql
  * @author huican.chen
  */
object ForeachRDDMySQLApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ForeachRDDMySQLApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val lines = ssc.socketTextStream("Spark01", 6189)
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val querySql = "SELECT t.word_count FROM wordcount t WHERE t.word = '"+record._1+"'"
          val queryResultSet = connection.createStatement().executeQuery(querySql)
          val hasNext = queryResultSet.next()
          println("MySQL had word:"+record._1+ " already  :  "+hasNext)

          if(!hasNext) {
            val insertSql = "insert into wordcount(word, word_count) values('" + record._1 + "'," + record._2 + ")"
            connection.createStatement().execute(insertSql)
          }else {
            val newWordCount = queryResultSet.getInt("word_count") + record._2
            val updateSql = "UPDATE wordcount SET word_count = "+newWordCount+" where word = '"+record._1+"'"
            connection.createStatement().execute(updateSql)
          }
        })
        connection.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }


  def createConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql//hadoop.node1:3306/test","root","360gst.com")
  }
}
