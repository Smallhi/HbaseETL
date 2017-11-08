package org.hhl.hbaseETL.example

import org.apache.hadoop.hbase.client.{Delete, Get, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.spark.sql.SparkSession
import org.hhl.hbaseETL.hbase._


/**
  * Created by huanghl4 on 2017/11/3.
  */
object HbaseSuit extends RowKey{
  // 获取SparkSession, spark 操作得入口
  val spark = SparkSession.builder()
    .appName(s"${this.getClass.getSimpleName}")
    .enableHiveSupport().getOrCreate()
 // 获取HbaseContext, bulk 操作 Hbase的入口
  val hBaseHelper = new HbaseHelper
  val hc = new HBaseContext(spark.sparkContext, hBaseHelper.getConf)

  val tableName = "GRAPH"
  val columnFamily = Seq("s")

// create table
// hbase bulk get/load/delete 效率和hbase 表region 个数以及数据分布是否均匀有直接关系
  // 建表时需综合考虑 region 个数，目标数据量，
  // region 个数过大而Hbase 集群节点过少导致数据频繁得换入换出
  // region 个数过小，导致无法充分发挥spark 并发得效率，所以需在建表时进行预分区
  // 预分区个数 需综合Hbase 集群节点个数，目标表大小，目标表上可能的操作进行设计。
  // 在用户画像标签系统中，
  // 1. 数据量：50G +
  // 2. hbase 集群： 3 个node , 64c + 128G 内存
  // 3. 目标表： 频繁初始化，支持大批量读，写，删。
  // 基于以上需求：region 数据设定为 50.

  def createTable = {
    hBaseHelper.nameSpace = "lenovo"
    hBaseHelper.execute(connection => {
      hBaseHelper.deleteHTable(connection, tableName)
      hBaseHelper.createHTable(connection, tableName, 50, columnFamily.toArray)
    })
  }


// 初始化数据， bulk load
  def initDate() = {
    // 清空，并重新创建表
    createTable
    // 准备数据，rdd 处理
    import spark.implicits._
    val rdd = spark.sql("select * from hive.graph").map(x => {
      val sid = x.getString(0)
      val id = x.getString(1)
      val idType = x.getString(3)
      (sid, id, idType)
    }).rdd
    // bulk load
    hc.bulkLoadThinRows[(String, String, String)](rdd,
      "lenovo:GRAPH",
      t => {
        val rowKey = rowKeyByMD5(t._2, t._3)
        val familyQualifiersValues = new FamiliesQualifiersValues

        val pk = t._2 + "|" + t._3
        // Hbase 存入两列，一列 PK 存 业务主键，一列 s 存 superid
        val column = List(("pk", pk), ("s", t._1))
        column.foreach(f => {
          val family: Array[Byte] = Bytes.toBytes(columnFamily.head)
          val qualifier = Bytes.toBytes(f._1)
          val value: Array[Byte] = Bytes.toBytes(f._2)
          familyQualifiersValues += (family, qualifier, value)
        })
        (new ByteArrayWrapper(rowKey), familyQualifiersValues)
      },
      10
    )
  }

    def bulkGet = {
      import spark.implicits._
      val rdd = spark.sql("select * from hive.graph").map(x => {
        val sid = x.getString(0)
        val id = x.getString(1)
        val idtype = x.getString(3)
        //Bytes.toBytes(sid)
        rowKeyByMD5(id,idtype)
      }).limit(20000000)
        //.sort() sort 后 repartition sort 可能会失效
        .repartition(100)
        //.sort() 小数据量排序后再去查询效果不明显
        .rdd
      val num1 = rdd.count()
      System.out.println( num1 + " query ")

     // val msg3 = DateUtils.localDateTime + "begin bulkGet"
      //System.out.println(msg3)
      val getRdd = hc.bulkGet[Array[Byte], String](
        TableName.valueOf(tableName),
        1000,
        rdd,
        record => {
          new Get(record)
        },
        (result: Result) => {
          if (!result.isEmpty) {
            val cell = result.getColumnLatestCell(Bytes.toBytes("s"), Bytes.toBytes("pk"))
            val sid = Bytes.toString(CellUtil.cloneValue(cell))
            sid
          } else {
            ("NONE")
          }
        }
      ).filter(x => (x != "NONE"))

      val t = getRdd.count()
      println(t)
//      val msg4 = DateUtils.localDateTime + "end bulkGet " + t + "rows"
//      System.out.println(msg4)
    }

    def bulkDelete = {
      import spark.implicits._
      val rdd = spark.sql("select * from hive.graph").map(x => {
        val id = x.getString(1)
        val idType = x.getString(3)
        //Bytes.toBytes(sid)
        rowKeyByMD5(id,idType)
      }).limit(20000000)
        //.sort() sort 后 repartition sort 可能会失效
        .repartition(100)
        //.sort() 小数据量排序后再去查询效果不明显
        .rdd
      val num1 = rdd.count()
      System.out.println( num1 + " query ")
      hc.bulkDelete[Array[Byte]](
        rdd,
        TableName.valueOf(tableName),
        putRecord => new Delete(putRecord),
        // 删除时，bulksize 10000 和1000 效率差不多，每个分区的执行时间差不多。
        1000)
    }

    def bulkPut = {
      import spark.implicits._
      val rdd_put = spark.sql("select * from hive.graph").map(x => {
        val sid = x.getString(0)
        val id = x.getString(1)
        val idType = x.getString(3)
        (sid, id, idType)
      }).repartition(100).rdd
      hc.bulkPut[(String,String,String)](
        rdd_put,
        TableName.valueOf(tableName),
        put => {
          val rowKey = rowKeyByMD5(put._2, put._3)
          val p = new Put(rowKey)
          val family: Array[Byte] = Bytes.toBytes("s")
          val pk = put._2 + "|" + put._3
          val pk_v = Bytes.toBytes(pk)
          p.addColumn(family,Bytes.toBytes("pk"),pk_v)
          p.addColumn(family,Bytes.toBytes("s"),Bytes.toBytes(put._1))
          p
        })
    }

  def rddImplicitsCall = {
    import org.hhl.hbaseETL.hbase.HBaseRDDFunctions._
    import spark.implicits._
    val rdd = spark.sql("select * from graph").map(x=>(rowKeyByMD5(x.getString(1),x.getString(1)))).
      rdd.hbaseBulkDelete(hc,TableName.valueOf("lenovo:GRAHP"),r =>new Delete(r),1000)
  }

}
