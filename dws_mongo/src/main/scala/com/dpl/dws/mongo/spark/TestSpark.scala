package com.dpl.dws.mongo.spark

import com.dpl.dws.common.utils.DateTimeUtil.timstampToDateTimeFormat
import com.dpl.dws.mongo.utils.MongoSparkSessionUtil
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

object TestSpark {

  def main(args: Array[String]): Unit = {
    val mongoDb = "sdk_pro"
    val table_name = "risk_app_summary_result"
    test(mongoDb,table_name)

  }


  def test(mongoDb:String,mongo_table:String): Unit ={
    val spark = MongoSparkSessionUtil.initSparkSession(mongoDb,mongo_table,getClass.getName,true)
    import spark.implicits._
    val df1: DataFrame = MongoSpark.load(spark)
    df1.printSchema()
val column = "createTime"
val tcolumn = "updateTime"
    val tsToString = udf(timstampToDateTimeFormat _)
    val df2: DataFrame = df1.drop("_class")
      .withColumn("_id",df1.col("_id").getField("oid"))
//      .withColumn("createTime", tsToString($"createTime".cast(TimestampType)))
//      .withColumn("createTime", tsToString($"createTime"))
      .withColumn(column, tsToString($"${column}"))
    .withColumn(tcolumn, tsToString($"${tcolumn}"))
    df2.printSchema()
    df2.show()

  }






}
