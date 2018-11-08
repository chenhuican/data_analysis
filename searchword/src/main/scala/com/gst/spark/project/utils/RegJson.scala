package com.gst.spark.project.utils

object RegJson {
  def regJson(json:Option[Any]) = json match {
    case Some(map: Map[String, Any]) => map
    case other => println("Error jsonStr")
  }

}
