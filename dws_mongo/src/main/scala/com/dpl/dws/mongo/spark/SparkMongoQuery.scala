package com.dpl.dws.mongo.spark

import java.text.SimpleDateFormat
import java.util.Date

import com.dpl.dws.common.table.mongo.MongoTable
import com.dpl.dws.common.utils.DateTimeUtil
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

object SparkMongoQuery {

  def queryAllFromMongoTable(spark:SparkSession,mongoTable:MongoTable): DataFrame ={

    if(mongoTable.gettableColumns().isEmpty){//查询所有列
      return MongoSpark.load(spark)
    }

    //查询指定列
    val table_columns = mongoTable.gettableColumns()
    var query = "{ '$project' : { '_id':1 "
    for(i <- 0 until table_columns.size){
      println(i,table_columns(i))
      query = query + ",'"+table_columns(i)+"': 1"
    }
    query = query + "} }"
    println("query:"+query)
    val rdd:MongoRDD[Document] = MongoSpark.load(spark.sparkContext)
    val aggregatedRdd= rdd.withPipeline(Seq(Document.parse(query)))
    val df = aggregatedRdd.toDF()
    println("withPipeline table Schema-------------------------------")
    df.printSchema()
    df.show(10)
    //  println("df.count:",df.count())
    df
  }

  def queryByIncDay(spark:SparkSession,mongoTable:MongoTable,inc_date:String): DataFrame ={
//    val begin_time=inc_date+" 00:00:00.000"
//    val end_time=inc_date+" 23:59:59.999"
    val begin_time=inc_date+" 09:11:31.000"
    val end_time=inc_date+" 10:11:31.999"
    val dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS")
    val begin_date_time=  dateFormat.parse(begin_time)
    val end_date_time=  dateFormat.parse(end_time)
   // val end_date_time=  new Date()
    println("begin_date_time:"+begin_date_time)
    println("end_date_time:"+end_date_time)
    queryByIncrementColumn(spark,mongoTable,begin_date_time,end_date_time)
  }
  def queryByIncrementColumn(spark:SparkSession,mongoTable:MongoTable,begin_date_time:Date,end_date_time:Date): DataFrame ={

    val begin_date_time_utc = DateTimeUtil.getIUTCTimestamp(begin_date_time)
    val end_date_time_utc = DateTimeUtil.getIUTCTimestamp(end_date_time)
    println("begin_date_time_UTC:"+begin_date_time_utc)
    println("end_date_time_UTC:"+end_date_time_utc)
     val query ="{'$match': {'"+mongoTable.getIncrementBy()+"':{'$gte':ISODate('"+begin_date_time_utc+"')," +
      "'$lte':ISODate('"+end_date_time_utc+"')}} }"
    println("query:"+query)
    val rdd:MongoRDD[Document] = MongoSpark.load(spark.sparkContext)
    var aggregatedRdd= rdd.withPipeline(Seq(Document.parse(query)))
    aggregatedRdd.toDF().show(10)

    if(!mongoTable.gettableColumns().isEmpty){ //查询指定列
      val table_columns = mongoTable.gettableColumns()
      var project = " {'$project' : { '_id':1 "
      for(i <- 0 until table_columns.size){
        println(i,table_columns(i))
        project = project + ",'"+table_columns(i)+"': 1"
      }
      project = project + "}}"
      println("project:"+project)
      aggregatedRdd= rdd.withPipeline(Seq(Document.parse(query),Document.parse(project)))
    }
    val df = aggregatedRdd.toDF()
    println("withPipeline table Schema-------------------------------")
    df.printSchema()
    df
  }






}
