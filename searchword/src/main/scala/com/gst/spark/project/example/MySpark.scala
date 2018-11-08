package com.gst.spark.project.example

import org.apache.spark.streaming.Seconds
import org.apache.spark.{SparkConf, SparkContext}

object MySpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySpark").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1,2,3,4,5,6,8,9,10,11,12,13,14,15,16,17,18,19,20)).map(_*3)

    val mappedRdd = rdd.filter(_>10).collect()

    println(rdd.reduce(_+_))

    for (arg <- mappedRdd) {
      println(arg+" ")
    }
    println("math is work")
  }

}
