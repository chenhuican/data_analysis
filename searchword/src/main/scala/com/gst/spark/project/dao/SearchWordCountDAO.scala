package com.gst.spark.project.dao

import com.gst.spark.project.domain.SearchWordCount
import com.gst.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object SearchWordCountDAO {
 val tableName = "f_searchwords"
 val cf = "info"
 val qualifer = "click_count"

 def save(list: ListBuffer[SearchWordCount]): Unit =  {
   val table = HBaseUtils.getInstance().getTable(tableName)

   for (ele <- list) {
     table.incrementColumnValue(Bytes.toBytes(ele.city_words),
       Bytes.toBytes(cf),
       Bytes.toBytes(qualifer),
       ele.click_count)
   }
 }

  def count(city_words: String): Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(city_words))
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    if(value == null) {
      0L
    }else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[SearchWordCount]
    list.append(SearchWordCount("755_竹子林分院","755","竹子林分院",1))
    list.append(SearchWordCount("20_骏景分院","20","骏景分院",1))
    list.append(SearchWordCount("20_感冒咳嗽","20","感冒咳嗽",1))
    save(list)

  }

}
