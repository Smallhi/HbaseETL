package org.hhl.hbaseETL.hbase

/**
  * Created by huanghl4 on 2017/11/8.
  */
trait Logging {

  def logWarning(msg:String) = println(msg)
  def logDebug(msg:String) = println(msg)
  def logInfo(msg:String) = println(msg)
  def logError(msg:String) = println(msg)
  def logError(msg:String,ex:Exception) = {
    println(msg)
    ex.printStackTrace()
  }
  def logWarning(msg:String,ex:Exception) = {
    println(msg)
    ex.printStackTrace()
  }
}
