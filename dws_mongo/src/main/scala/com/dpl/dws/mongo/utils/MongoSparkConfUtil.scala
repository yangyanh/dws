package com.dpl.dws.mongo.utils

import com.dpl.dws.common.constant.Constant
import org.apache.spark.SparkConf

object MongoSparkConfUtil {
  private val sdk_pro_db = "sdk_pro"


  def initOutputConf(mongo_db:String,mongo_table:String,islocal:Boolean): SparkConf ={
    val conf = new SparkConf()
    if(islocal)conf.setMaster("local[*]")
    //如何你的密码中有@符号 请用%40代替
    conf.set("spark.mongodb.output.uri", Constant.mongo_output_url+"/"+mongo_db+"."+mongo_table+"?&readPreference=secondaryPreferred")
  }

  def initSdkProInputConf(mongo_table:String,islocal:Boolean): SparkConf ={
    initInputConf(sdk_pro_db,mongo_table,islocal)
  }

  def initInputConf(mongo_db:String,mongo_table:String,islocal:Boolean): SparkConf ={
    val conf = new SparkConf()
    if(islocal)conf.setMaster("local[*]")
    //如何你的密码中有@符号 请用%40代替
    conf.set("spark.mongodb.input.uri", Constant.mongo_input_url+"/"+mongo_db+"."+mongo_table+"?readPreference=primaryPreferred")
  }



}
