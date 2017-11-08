package org.hhl.hbaseETL.example

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SparkSession}
import org.hhl.hbaseETL.hbase._

/**
  * Created by huanghl4 on 2017/11/3.
  */
class DataBaseSuit extends  RowKey{

  // 获取SparkSession, spark 操作得入口
  val spark = SparkSession.builder()
    .appName(s"${this.getClass.getSimpleName}")
    .enableHiveSupport().getOrCreate()
  // 获取HbaseContext, bulk 操作 Hbase的入口
  val hBaseHelper = new HbaseHelper
  val hc = new HBaseContext(spark.sparkContext, hBaseHelper.getConf)
  val tableName = "lenovo"
  val columnFamily = Seq("cf")
  val table_PK = Seq("column1","column2")

  val schema = spark.read.table("").schema

  val columnNames = schema.map(_.name)

  def familyQualifierToByte:Set[(Array[Byte],Array[Byte],String)] ={
    if (columnFamily == null || columnNames == null) throw new Exception("null can't be convert to Bytes")
    columnFamily.map(x=>Bytes.toBytes(x)).zip(columnNames.map(x=>(Bytes.toBytes(x),x))).map(x=>(x._1,x._2._1,x._2._2)).toSet
  }

  def createTable = {
    hBaseHelper.nameSpace = "lenovo"
    hBaseHelper.execute(connection => {
      hBaseHelper.deleteHTable(connection, tableName)
      hBaseHelper.createHTable(connection, tableName, 50, columnFamily.toArray)
    })
  }

  def insertOrUpdate = {
    val rdd = spark.read.table("").rdd
    hc.bulkLoadThinRows[Row](rdd,
      tableName,
      r => {
        val rawPK = new StringBuilder
        for(c<- table_PK) rawPK.append(r.getAs[String](c))
        val rk = rowKeyByMD5(rawPK.toString)
        val familyQualifiersValues = new FamiliesQualifiersValues

        val fq = familyQualifierToByte
        for(c<- fq) {
          val family = c._1
          val qualifier = c._2
          val value = Bytes.toBytes(r.getAs[String](c._3))
          familyQualifiersValues += (family, qualifier, value)
        }
        (new ByteArrayWrapper(rk), familyQualifiersValues)
      },
      10)
  }

  def deleteByPK = {
    val rdd = spark.read.table("").rdd
    hc.bulkDelete[Row](rdd,
      TableName.valueOf(tableName),
      r => {
        val rawPK = new StringBuilder
        for(c<- table_PK) rawPK.append(r.getAs[String](c))
        val rk = Bytes.toBytes(rawPK.toString)
        new Delete(rk)
      },
      1000)
  }
}
