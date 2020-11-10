package com.dpl.dws.hudi

import com.dpl.dws.mongo.utils.MongoSparkConfUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HudiSparkUtil {
  private val islocal = true

  def initSparkSession(): SparkSession ={
    val conf = new SparkConf()
    if(islocal){
      conf.setMaster("local[*]")
    }else{
    //  conf.setMaster("yarn client")
    }
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  }

  def initMongoSparkSession(mongodb:String,mongo_table:String,appName:String): SparkSession ={
    val conf = MongoSparkConfUtil.initInputConf(mongodb,mongo_table,islocal).setAppName(appName)
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  }

}
