package com.dpl.dws.mongo.spark

import java.util.Date

import com.dpl.dws.common.table.mongo.MongoTable
import com.dpl.dws.common.table.mongo.sdk_pro.{risk_app_summary_result, sdk_user_contact}
import com.dpl.dws.common.utils.DateTimeUtil.{timstampToDateTimeFormat, timstampToYearMonth}
import com.dpl.dws.mongo.utils.MongoSparkSessionUtil
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkHudiDataFrame {
  private val islocal = true

  def main(args: Array[String]): Unit = {
//    testgetDataFrameFromMongo(new risk_app_summary_result())
    testgetDataFrameFromMongoByDate(new sdk_user_contact(),"20200624")
//    testgetDataFrameFromMongoByDate("20191102")
  }

  def testgetDataFrameFromMongo(mongoTable:MongoTable): Unit ={
    val spark = MongoSparkSessionUtil.initSparkSession(mongoTable.getMongoDb(),mongoTable.getTableName(),getClass.getName,islocal)
    spark.sparkContext.setLogLevel("INFO")
    var df = getDataFrameFromMongo(spark,mongoTable)
//    var df = getDataFrameFromMongo(new sdk_user_contact())
    df.show(10)
  }

  def testgetDataFrameFromMongoByDate(mongoTable:MongoTable,inc_date:String): Unit ={
    val spark = MongoSparkSessionUtil.initSparkSession(mongoTable.getMongoDb(),mongoTable.getTableName(),getClass.getName,islocal)
    spark.sparkContext.setLogLevel("INFO")
//    var df = getDataFrameFromMongoByDate(new risk_app_summary_result(),inc_date)
        var df = getDataFrameByIncDay(spark,mongoTable,inc_date)
   // df.show(10)
  }

  def getDataFrameFromMongo(spark:SparkSession,mongoTable:MongoTable): DataFrame ={
    val df1 = SparkMongoQuery.queryAllFromMongoTable(spark,mongoTable)
    if(df1==null || df1.columns.size==0)return df1
     formatDataFrame(spark,df1,mongoTable)
  }

  def getDataFrameByIncDay(spark:SparkSession,mongoTable:MongoTable,inc_date:String): DataFrame ={
    val df1:DataFrame = SparkMongoQuery.queryByIncDay(spark,mongoTable,inc_date)
    if(df1==null || df1.columns.size==0)return df1
    formatDataFrame(spark,df1,mongoTable)
  }

  def getIncrementDataFrameByIncrementColumn(spark:SparkSession,mongoTable:MongoTable,begin_date_time:Date,end_date_time:Date): DataFrame ={
    val df1:DataFrame = SparkMongoQuery.queryByIncrementColumn(spark,mongoTable,begin_date_time,end_date_time)
    if(df1==null || df1.columns.size==0)return df1
    formatDataFrame(spark,df1,mongoTable)
  }


  val tsToString = udf(timstampToDateTimeFormat _)
  val tsToYearMonth = udf(timstampToYearMonth _)

  def formatDataFrame(spark:SparkSession,df1: DataFrame,mongoTable:MongoTable): DataFrame ={
      import spark.implicits._
      df1.printSchema()
       var df: DataFrame = df1.drop("_class")
      if (df.columns.contains("_id")){
        df = df.withColumn("_id",df1.col("_id").getField("oid"))
      }

      if(mongoTable.getIsPartition()){//是否需要分区
        df=df.withColumn("dt", tsToYearMonth($"${mongoTable.getPartitionBy()}"))
      }
    val time_columns = mongoTable.getTimeColumns()
    if(!time_columns.isEmpty){//时间戳字段转换成String
      for ( tcolumn <- time_columns) {
        df=df.withColumn(tcolumn, tsToString($"${tcolumn}"))
      }
    }

    df.printSchema()
    df
  }

}
