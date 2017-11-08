package org.hhl.hbaseETL.hbase

import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}

/**
  * Created by huanghl4 on 2017/11/3.
  */
trait RowKey {
  def rowKeyWithHashPrefix(column: String*): Array[Byte] = {
    val rkString = column.mkString("")
    val hash_prefix = getHashCode(rkString)
    val rowKey = Bytes.add(Bytes.toBytes(hash_prefix), Bytes.toBytes(rkString))
    rowKey
  }

  def rowKeyWithMD5Prefix(separator:String,length: Int,column: String*): Array[Byte] = {
    val columns = column.mkString(separator)

    var md5_prefix = MD5Hash.getMD5AsHex(Bytes.toBytes(columns))
    if (length < 8){
      md5_prefix = md5_prefix.substring(0, 8)
    }else if (length >= 8 || length <= 32){
      md5_prefix = md5_prefix.substring(0, length)
    }
    val row = Array(md5_prefix,columns)
    val rowKey = Bytes.toBytes(row.mkString(separator))
    rowKey
  }

  def rowKeyByMD5(column: String*): Array[Byte] = {
    val rkString = column.mkString("")
    val md5 = MD5Hash.getMD5AsHex(Bytes.toBytes(rkString))
    val rowKey = Bytes.toBytes(md5)
    rowKey
  }

  def rowKey(column:String*):Array[Byte] = Bytes.toBytes(column.mkString(""))

  private def getHashCode(field: String): Short ={
    (field.hashCode() & 0x7FFF).toShort
  }
}
