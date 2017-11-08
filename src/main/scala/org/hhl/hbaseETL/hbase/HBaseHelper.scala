package org.hhl.hbaseETL.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit

/**
  * Created by huanghl4 on 2017/11/3.
  */
class HbaseHelper {
  var nameSpace = "lenovo"
  def getConf: Configuration = {
    val conf: Configuration = HBaseConfiguration.create
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "node81.it.leap.com,node82.it.leap.com")
    conf.set("hbase.client.keyvalue.maxsize", "1048576000") //500M
    conf.set("zookeeper.znode.parent","/hbase-unsecure")
    conf
  }

  def getConnection: Connection = {
    val conf = getConf
    ConnectionFactory.createConnection(conf)

  }

  def execute(action: Connection => Unit): Unit = {

    val connection = getConnection
    try {
      action(connection)
    }
    finally {
      connection.close()
    }
  }

  def deleteHTable(connection: Connection, tn: String): Unit = {
    val tableName = TableName.valueOf(nameSpace + ":" + tn)
    val admin = connection.getAdmin
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
  }
  /**
    * Hbase自带了两种pre-split的算法,分别是 HexStringSplit 和  UniformSplit
    * 如果我们的row key是十六进制的字符串作为前缀的,就比较适合用HexStringSplit
    * @param tablename 表名
    * @param regionNum 预分区数量
    * @param columns 列簇数组
    */

  def createHTable(connection: Connection, tablename: String,regionNum: Int, columns: Array[String]): Unit = {

    val hexsplit: HexStringSplit = new HexStringSplit()
    val splitkeys: Array[Array[Byte]] = hexsplit.split(regionNum)

    val admin = connection.getAdmin

    val tableName = TableName.valueOf(nameSpace + ":" + tablename)

    if (!admin.tableExists(tableName)) {

      if(!admin.getNamespaceDescriptor(nameSpace).getName.equals(nameSpace))
        admin.createNamespace(NamespaceDescriptor.create(nameSpace).build())

      val tableDescriptor = new HTableDescriptor(tableName)

      if (columns != null) {
        columns.foreach(c => {
          val hcd = new HColumnDescriptor(c.getBytes()) //设置列簇
          hcd.setMaxVersions(1)
          hcd.setCompressionType(Algorithm.GZ) //设定数据存储的压缩类型.默认无压缩(NONE)
          tableDescriptor.addFamily(hcd)
        })
      }
      admin.createTable(tableDescriptor,splitkeys)
    }

  }


}
