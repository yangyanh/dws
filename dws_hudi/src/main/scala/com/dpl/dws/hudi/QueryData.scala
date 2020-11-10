package com.dpl.dws.hudi

import com.dpl.dws.common.table.BeanUtil
import com.dpl.dws.common.table.mongo.MongoTable
import org.apache.hudi.DataSourceReadOptions


object QueryData {
 // val basePath= "/user/hive/warehouse/mongodb/hudi/sdk_pro/"
  val mongo_table = "risk_app_summary_result"

  private val basePath = "/user/hive/warehouse/hudi/"
  private val hivedb_Prefix = "mon_"

  def main(args: Array[String]): Unit = {
    val mongoTable = BeanUtil.getMongoTable("sdk_pro",mongo_table)
    queryTable(mongoTable)
  }

  def queryTable(mongoTable:MongoTable): Unit ={
    val spark = HudiSparkUtil.initSparkSession()
//    val path = basePath +mongo_table+"_hudi/*/*"
val path = basePath+hivedb_Prefix+mongoTable.getMongoDb()+"/"+mongoTable.getTableName()+"/*.parquet"
    val roViewDF = spark.read.format("org.apache.hudi").load(path)
    roViewDF.createOrReplaceTempView("hudi_ro_table")
    val result = spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, `_id`,appname,apppackagename,createtime,updatetime  from  hudi_ro_table limit 10")
    val result2 = spark.sql("select count(*)  from  hudi_ro_table")
    result.printSchema()
    result.show(10)
    result2.show()
  }


  def readIncrementView(): Unit = {
    val spark = HudiSparkUtil.initSparkSession()
    spark.read
      .format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20200806155654")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, "20200806161512")
      .load("hdfs://localhost:8020/user/hive/warehouse/hudi.db/hudi_hive_sync")
      .show(false)
  }




}
