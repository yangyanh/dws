package com.dpl.dws.hudi

import com.dpl.dws.common.utils.DateTimeUtil._
import com.dpl.dws.hudi.HudiOptions.getQuickstartWriteConfigs
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.bson.Document


object SynByHudi_bak {

  private val basePath = "/user/hive/warehouse/mongodb/hudi/sdk_pro/"
  def main(args: Array[String]): Unit = {
    val tablename="risk_app_summary_result"
    println("tablenmae:"+tablename)
//    test(tablename,"20201022","updateTime")
//    val optype = "overwrite"
    val optype = args(0)
    if("overwrite" == optype){
      syn_origin_table_partition(tablename)
    }else  if("append" == optype){
      val day = args(1)
      syn_upsert_table_partition(tablename,day,"updateTime")
    }
  }

  def test(mongo_table:String,inc_date:String,incrementBy:String): Unit ={
    println(mongo_table,inc_date,incrementBy)
    val spark = HudiSparkUtil.initMongoSparkSession("sdk_pro",mongo_table,"testapp")
    spark.sparkContext.setLogLevel("INFO")


    val begin_date_time_UTC = getDayBeginTimeStamp(inc_date)
    val end_date_time_UTC = getDayEndTimeStamp(inc_date)

    val query = "{ '$match': {'"+incrementBy+"':{'$gte':ISODate('"+begin_date_time_UTC+"')," +
      "'$lte':ISODate('"+end_date_time_UTC+"')}} }"
    println("query:"+query)

    val rdd:MongoRDD[Document] = MongoSpark.load(spark.sparkContext)

    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse(query)))
    val count = aggregatedRdd.count
    println("count___"+count)

    if(count==0){
      spark.stop()
      return
    }

    //将DF写入到Hive中
    //选择Hive数据库
    val df1 = aggregatedRdd.toDF()
    df1.printSchema()
    val df: DataFrame = formatDataFrame(spark,df1)
    df.show(10)
  }



  def syn_origin_table_normal(mongo_table:String): Unit ={
    val spark =  HudiSparkUtil.initMongoSparkSession("sdk_pro",mongo_table,"testapp")
    spark.sparkContext.setLogLevel("INFO")
    val df1: DataFrame = MongoSpark.load(spark)
    df1.printSchema()
    val df: DataFrame = formatDataFrame(spark,df1)
    df.show(10)

    df.write.format("org.apache.hudi")
      .options(getQuickstartWriteConfigs)
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_id")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "updateTime")
      // 设置要同步的hive库名
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "mongo_sdk_pro")
      // 设置要同步的hive表名
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, mongo_table+"_hive")
      // 设置数据集注册并同步到hive
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // hudi表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, mongo_table+"_hudi")
      .mode(SaveMode.Overwrite)
      .save(basePath+mongo_table+"_hudi")

  }

  def syn_origin_table_partition(mongo_table:String): Unit ={
    val spark =  HudiSparkUtil.initMongoSparkSession("sdk_pro",mongo_table,"testapp")
    spark.sparkContext.setLogLevel("INFO")
    val df1: DataFrame = MongoSpark.load(spark)
    df1.printSchema()
    df1.show(10)
    val df: DataFrame = formatDataFrame(spark,df1)
    df.show(10)

    df.write.format("org.apache.hudi")
      .options(getQuickstartWriteConfigs)
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_id")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "updateTime")
      // 分区列设置
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
      // 设置要同步的hive库名
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "mongo_sdk_pro")
      // 设置要同步的hive表名
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, mongo_table+"_hive")
      // 设置数据集注册并同步到hive
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 设置要同步的分区列名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt")
      // hudi表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, mongo_table+"_hudi")
      .mode(SaveMode.Overwrite)
      .save(basePath+mongo_table+"_hudi")

  }

  def syn_upsert_table_partition(mongo_table:String,inc_date:String,incrementBy:String): Unit ={

    println(mongo_table,inc_date,incrementBy)
    val spark =  HudiSparkUtil.initMongoSparkSession("sdk_pro",mongo_table,"testapp")
    spark.sparkContext.setLogLevel("INFO")


    val begin_date_time_UTC = getDayBeginTimeStamp(inc_date)
    val end_date_time_UTC = getDayEndTimeStamp(inc_date)
    println("begin_date_time_UTC:"+begin_date_time_UTC)
    println("end_date_time_UTC:"+end_date_time_UTC)

    val query = "{ '$match': {'"+incrementBy+"':{'$gte':ISODate('"+begin_date_time_UTC+"')," +
      "'$lte':ISODate('"+end_date_time_UTC+"')}} }"
    println("query:"+query)

    val rdd:MongoRDD[Document] = MongoSpark.load(spark.sparkContext)

    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse(query)))
    val count = aggregatedRdd.count
    println("count___"+count)

    if(count==0){
      spark.stop()
      return
    }

    //将DF写入到Hive中
    //选择Hive数据库
    val df1 = aggregatedRdd.toDF()
    df1.printSchema()
    val df: DataFrame = formatDataFrame(spark,df1)
    df.show(10)

    df.write.format("org.apache.hudi")
      .options(getQuickstartWriteConfigs)
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_id")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "updateTime")
      // 分区列设置
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
      // 设置要同步的hive库名
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "mongo_sdk_pro")
      // 设置要同步的hive表名
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, mongo_table+"_hive")
      // 设置数据集注册并同步到hive
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 设置要同步的分区列名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt")
      // hudi表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, mongo_table+"_hudi")
      .mode(SaveMode.Append)
      .save("/user/hive/warehouse/mongodb/hudi/sdk_pro/"+mongo_table+"_hudi")

  }

  def formatDataFrame(spark:SparkSession,df1: DataFrame): DataFrame ={
    import spark.implicits._
    val tsToString = udf(timstampToDateTimeFormat _)
    val tsToYearMonth = udf(timstampToYearMonth _)
    val df2: DataFrame = df1.drop("_class")
      //.withColumn("_id",df1.col("_id").getField("oid"))
      .withColumn("dt", tsToYearMonth($"createTime".cast(TimestampType)))
      .withColumn("createTime", tsToString($"createTime".cast(TimestampType)))
    df2.printSchema()
    df2
  }



}
