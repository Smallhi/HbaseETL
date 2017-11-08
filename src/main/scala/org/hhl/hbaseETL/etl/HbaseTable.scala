package org.hhl.hbaseETL.etl

import org.apache.hadoop.hbase.client.{Delete, Get, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.hhl.hbaseETL.etl.HbaseTable.Schema
import org.hhl.hbaseETL.hbase._

import scala.collection.mutable.ListBuffer

/**
  * Created by huanghl4 on 2017/11/3.
  */
class HbaseTable(@transient private val spark:SparkSession,
                 @transient  private val hc:HBaseContext,
                 schema:Schema){
  val hbaseHelper = new HbaseHelper
  private var bulkDeleteSize = HbaseConstants.defaultBulkDeleteSize
  private var bulkGetSize = HbaseConstants.defaultBulkGetSize
  private var bulkPutSize = HbaseConstants.defaultBulkPutSize
  private var bulkLoadRegionNumbers = HbaseConstants.defaultBulkLoadRegionNumbers
  // HbaseCell(KV) -> (rowKey,columnFamily,qualifier,value)
  type hbaseRow = (String, String, String, String)

  def setBulkDeleteSize(bulkSize:Int):Unit = {this.bulkDeleteSize = bulkSize}
  def setBulkGetSize(bulkSize:Int) :Unit= {this.bulkGetSize = bulkSize}
  def setBulkPutSize(bulkSize:Int) :Unit= {this.bulkGetSize = bulkSize}
  def setBulkLoadRegionNumbers(rn:Int) :Unit= {this.bulkGetSize = rn}


  private def createTable = {
    // 建表之前，首先通过Hbase shell 创建 nameSpace
  hbaseHelper.nameSpace = schema.nameSpace
    hbaseHelper.execute(connection => {
      hbaseHelper.deleteHTable(connection, schema.getTableName)
      hbaseHelper.createHTable(connection, schema.getTableName, schema.regionNumbers, schema.columnFamily.toArray)
  })
}

  def regionPartitioner = {
    val regionLocator =  hbaseHelper.getConnection.getRegionLocator(TableName.valueOf(schema.getTableName))
    val startKeys = regionLocator.getStartKeys
    if (startKeys.length == 0) {
      //println("Table " + regionLocator + " was not found")
    }
     new BulkPartitioner(startKeys,10)
  }

  /**
    *
    * @param rdd
    * @param rowKeyFunction
    * @tparam T
    */
  def bulkDeleteRDD[T](rdd: RDD[T],
                       rowKeyFunction: (T) => Array[Byte]) = {
    val rowKeyRDD = rdd.map(x => rowKeyFunction(x))
    hc.bulkDelete[Array[Byte]](
      rowKeyRDD,
      TableName.valueOf(schema.getTableName),
      record => (new Delete(record)),
      bulkDeleteSize)
  }

  /**
    *
    * @param rdd
    * @param mkRowKey,将指定RDD 得某几列转为 rowKey
    * @tparam T
    * @return
    */
  def bulkGetRDD[T](rdd: RDD[T],
                    mkRowKey: T => Array[Byte]): RDD[hbaseRow] = {
    // 为提高效率，对RDD进行重分区
    val rowKeyRDD = rdd.map(x => mkRowKey(x))
    var num = (rowKeyRDD.count() / bulkGetSize).toInt
    num = if (num > 0) num else 1
    rowKeyRDD.repartition(num).cache()

    hc.bulkGet[Array[Byte], List[hbaseRow]](
      TableName.valueOf(schema.getTableName),
      bulkGetSize,
      rowKeyRDD,
      record => (new Get(record)),
      result => {
        val columnBuffer = new ListBuffer[hbaseRow]
        if (!result.isEmpty) {
          val rowKey = Bytes.toString(result.getRow)
          for (c <- schema.familyQualifierToByte) {
            val family = Bytes.toString(c._1)
            val qualifier = Bytes.toString(c._2)
            val cell = result.getColumnLatestCell(c._1, c._2)
            val value = Bytes.toString(CellUtil.cloneValue(cell))
            columnBuffer.append((rowKey, family, qualifier, value))
          }
        }
        columnBuffer.toList
      }).flatMap(x => x)
  }

  /**
    * Spark Row 数据更新到 Hbase 中
    * @param rdd
    * @param mkRowKey，将Row 转换为Hbase RowKey
    * @param mkHbaseRow，将Row 转换为FamiliesQualifiersValues，一棵红黑树（排序的）
    */
  def insertElseUpdate[Row](rdd:RDD[Row],
                     mkRowKey: Row => Array[Byte],
                     mkHbaseRow:Row =>FamiliesQualifiersValues) = {
    hc.bulkLoadThinRows[Row](
      rdd,
      schema.getTableName,
      r => {
        val rk = mkRowKey(r)
       val familyQualifiersValues = mkHbaseRow(r)
        (new ByteArrayWrapper(rk), familyQualifiersValues)
      },
      bulkLoadRegionNumbers
    )
  }

  def tableInit[Row](rdd:RDD[Row],
                mkRowKey: Row => Array[Byte],
                mkHbaseRow:Row =>FamiliesQualifiersValues) ={
    // 创建表
    createTable
    // 初始化数据
    insertElseUpdate[Row](rdd,mkRowKey,mkHbaseRow)
  }

  /**
    * 全表扫描Hbase 表，通过Spark SQL 解决Hbase 不能Join 的问题
    * @return
    */
  def hbaseScan: RDD[hbaseRow] = {
    val scan = new Scan()
    scan.setCacheBlocks(false) // 全表扫描，不需用内存缓存，LRU算法
    //scan.setBatch(2)
    scan.setCaching(bulkGetSize)
    hc.hbaseRDD(
      TableName.valueOf(schema.getTableName),
      scan,
      r => {
        val result = r._2
        val columnBuffer = new ListBuffer[hbaseRow]
        val rowKey = Bytes.toString(result.getRow)
        for (c <- schema.familyQualifierToByte) {
          val family = Bytes.toString(c._1)
          val qualifier = Bytes.toString(c._2)
          val cell = result.getColumnLatestCell(c._1, c._2)
          if (cell != null) {
            val value = Bytes.toString(CellUtil.cloneValue(cell))
            columnBuffer.append((rowKey, family, qualifier, value))
          }
        }
        columnBuffer.toList
      }).flatMap(x => x)
  }

}

object HbaseTable {
  def apply(spark: SparkSession, hc: HBaseContext,schema: Schema): HbaseTable = new HbaseTable(spark,hc,schema)

  case class Schema(nameSpace:String,
                    tableName:String,
                    columnFamily:Seq[String],
                    columns:Seq[String],
                    regionNumbers:Int) {
    def getTableName : String = {
      this.nameSpace + ":" + this.tableName
    }

    def familyQualifierToByte:Set[(Array[Byte],Array[Byte])] ={
      if (columnFamily == null || columns == null) throw new Exception("null can't be convert to Bytes")
      columnFamily.map(x=>Bytes.toBytes(x)).zip(columns.map(x=>Bytes.toBytes(x))).toSet
    }

    def strToBytes(str:String) = Bytes.toBytes(str)

    def bytesToString(arr:Array[Byte]) = Bytes.toString(arr)
  }
}
