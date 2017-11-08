package org.hhl.hbaseETL.example

import org.apache.spark.sql.{Row, SparkSession}
import org.hhl.hbaseETL.etl.HbaseTable
import org.hhl.hbaseETL.etl.HbaseTable.Schema
import org.hhl.hbaseETL.hbase._

/**
  * Created by huanghl4 on 2017/11/3.
  */
object HbaseETLSuit extends RowKey {
  // 获取SparkSession, spark 操作得入口
  val spark = SparkSession.builder()
    .appName(s"${this.getClass.getSimpleName}")
    .enableHiveSupport().getOrCreate()
  // 获取HbaseContext, bulk 操作 Hbase的入口
  val hBaseHelper = new HbaseHelper
  val hc = new HBaseContext(spark.sparkContext, hBaseHelper.getConf)
  // Hbase 表定义
  val nameSpace = "lenovo"
  val tableName = "GRAPH"
  val columnFamily = Seq("cf")
  // 获取源表得Schema 信息
  val columns = spark.sql("select * from hive.graph").schema.map(_.name)
  val schema = Schema(nameSpace, tableName, columnFamily, columns, 50)
  val data = spark.sql("select * from hive.graph").rdd
  // (sid:String,id:String,idType:String)
  // 创建Hbase Table 实例
  val ht = HbaseTable(spark, hc, schema)
  // 初始化数据测试
  ht.tableInit[Row](data, mkRowKey, mkHbaseRow)

  // Spark Join
  // fixme Scan 返回得结构为SparkRow
  import spark.implicits._
  val t1 = ht.hbaseScan.toDF("id","idtype")
  val t2 = spark.sql("select * from t1")

  val join = t1.join(t2,Seq("id","idtype"))

  // 构造Hbase Table 的 rowkey 规则
  def mkRowKey(r: Row): Array[Byte] = {
    // 业务要求是用 id+idtype 得Md5 作为主键
    val rawRK = r.getAs[String]("id") + r.getAs[String]("idType")
    rowKeyByMD5(rawRK)
  }

  // 构造Hbase Table 的 row 的规则
  def mkHbaseRow(r: Row): FamiliesQualifiersValues = {
    val rk = this.mkRowKey(r)
    val familyQualifiersValues = new FamiliesQualifiersValues
    var i = 0
    for (c <- schema.familyQualifierToByte.toList) {
      val family = c._1
      val qualifier = c._2
      val value: Array[Byte] = schema.strToBytes(r.getString(i))
      familyQualifiersValues += (family, qualifier, value)
      i = i + 1
    }
    familyQualifiersValues
  }
}
