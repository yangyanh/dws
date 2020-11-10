package com.dpl.dws.mongo.utils

import org.apache.spark.sql.SparkSession

object MongoSparkSessionUtil {

  def initSparkSession(mongo_db:String,mongo_table:String,appName:String,islocal:Boolean): SparkSession ={
    val conf = MongoSparkConfUtil.initInputConf(mongo_db,mongo_table,islocal).setAppName(appName)
    SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  }

}
