package com.dpl.dws.hudi

import java.util.Date

import com.dpl.dws.common.table.BeanUtil
import com.dpl.dws.common.table.mongo.MongoTable
import com.dpl.dws.common.utils.DateTimeUtil
import com.dpl.dws.hudi.HudiOptions.getQuickstartWriteConfigs

import com.dpl.dws.mongo.spark.SparkHudiDataFrame
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.hive.{MultiPartKeysValueExtractor, NonPartitionedExtractor}
import org.apache.hudi.keygen.{NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.log4j.Logger
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}



object SynMongoByHudi {
  val logger = Logger.getLogger("SynMongoByHudi")
  private val basePath = "/user/hive/warehouse/hudi/"
//  private val basePath = "hdfs://dev110:8020/user/hive/warehouse/hudi/"
  private val hivedb_Prefix = "mon_"


  def main(args: Array[String]): Unit = {
//    val  mongoDb="sdk_pro"
//    val table_name = "risk_app_summary_result"
//    val optype = "2"


   val  mongoDb=args(0)
   val  table_name=args(1)
    val optype = args(2)

    val mongoTable = BeanUtil.getMongoTable(mongoDb,table_name)
    if(mongoTable==null){
      println("could not found table :",mongoDb,table_name)
      logger.error(" logger.error :could not found table :",mongoDb,table_name)
      logger.info(" logger.info :could not found table :",mongoDb,table_name)
      return
    }
    println("tablenmae:"+mongoTable.toString())

    if("0" == optype){
      syn_origin_table(mongoTable)
    }else  if("1" == optype){
      syn_upsert_table(mongoTable)
    }else  if("2" == optype){
//          queryTable(mongoTable)
//          getMaxUpdateTime(mongoTable)
val tablePath = basePath +hivedb_Prefix+mongoTable.getMongoDb()+"/"+mongoTable.getTableName()
      getMaxCommitTime(tablePath)
    }else{
      println("optype is error :",optype)
      logger.error(" logger.error :optype is error :"+optype)
    }
  }


  def syn_origin_table(mongoTable:MongoTable): Unit ={
    val spark = HudiSparkUtil.initMongoSparkSession(mongoTable.getMongoDb(),mongoTable.getTableName(),getClass.getName+mongoTable.getTableName())
    val df: DataFrame = SparkHudiDataFrame.getDataFrameFromMongo(spark,mongoTable)
    df.show(10)

    val dw = dataFrameToDataFrameWriter(df,mongoTable)
      dw.mode(SaveMode.Overwrite)
      .save(basePath+hivedb_Prefix+mongoTable.getMongoDb()+"/"+mongoTable.getTableName())
    spark.stop()
  }

  def syn_upsert_table(mongoTable:MongoTable): Unit ={

    println("syn_upsert_table_partition",mongoTable)
    val beginUpdateTime = getMaxUpdateTime(mongoTable)
   // val beginUpdateTime =  DateTimeUtil.parsedateTime("2020-10-28 15:58:51.278")
    val endUpdateTime = new Date()
    val spark = HudiSparkUtil.initMongoSparkSession(mongoTable.getMongoDb(),mongoTable.getTableName(),"syn_upsert_table_partition_"+mongoTable.getTableName())

    val incDataFrame = SparkHudiDataFrame.getIncrementDataFrameByIncrementColumn(spark,mongoTable,beginUpdateTime,endUpdateTime)
    incDataFrame.printSchema()
  //  incDataFrame.show(10)

    val dw = dataFrameToDataFrameWriter(incDataFrame,mongoTable)

      .mode(SaveMode.Append)
        .save(basePath+hivedb_Prefix+mongoTable.getMongoDb()+"/"+mongoTable.getTableName())
    spark.stop()
  }


  def dataFrameToDataFrameWriter(df:DataFrame,mongoTable: MongoTable): DataFrameWriter[Row] ={
    val dw = df.write.format("org.apache.hudi")
      .options(getQuickstartWriteConfigs)
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_id")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, mongoTable.getIncrementBy())

      // 设置要同步的hive库名
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, hivedb_Prefix+mongoTable.getMongoDb())
      // 设置要同步的hive表名
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, mongoTable.getTableName())
      // 设置数据集注册并同步到hive
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      //Increase hoodie.keep.min.commits=3 to be greater than hoodie.cleaner.commits.retained=3.
      // Otherwise, there is risk of incremental pull missing data from few instants.
    .option(HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP,3)//保留最近三次提交的数据
    .option(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP,5)//保留.hoodie 下的文件最小个数
    .option(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP_PROP,8)//保留.hoodie 下的文件最大个数

    //1.判断当前表是否为分区表
    if(mongoTable.getIsPartition()){
      // 分区列设置
      dw.option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
        // 设置当分区变更时，当前数据的分区目录是否变更
        .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
        // 设置要同步的分区列名
        .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt")
        // 用于将分区字段值提取到Hive分区列中的类,这里我选择使用当前分区的值同步
        .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY,classOf[MultiPartKeysValueExtractor].getName)
        //hudi表主键生成
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[SimpleKeyGenerator].getName)
    } else {
      //分区表与非分区表的主键生成策略不同，需要注意
      dw.option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[NonpartitionedKeyGenerator].getName)
        //hive_sync.partition_extractor_class
        .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[NonPartitionedExtractor].getName)
    }

    // hudi表名称设置
    dw.option(HoodieWriteConfig.TABLE_NAME, mongoTable.getTableName()+"_hudi")
  }

  def getMaxCommitTime(tablePath:String): Long ={
    val commitPath= tablePath+"/.hoodie/*.commit"
    val spark = HudiSparkUtil.initSparkSession()

    val fileRDD = spark.sparkContext.newAPIHadoopFile[LongWritable, Text, TextInputFormat](commitPath)
    val hadoopRDD = fileRDD.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
    val fileAdnLine = hadoopRDD.mapPartitionsWithInputSplit((inputSplit: InputSplit, iterator: Iterator[(LongWritable, Text)]) => {
      val file = inputSplit.asInstanceOf[FileSplit]
      val fileName = file.getPath.getName//  文件名
      val commit = fileName.substring(0,fileName.indexOf(".commit")).toLong
      println("fileName:"+fileName)
      println("commit:"+commit)
      iterator.map(x => {
    //    println("xxxx:"+x)
        commit
      })
    })
    fileAdnLine.reduce(_ max _)
  }

  def getMaxUpdateTime(mongoTable:MongoTable): Date = {
//    val spark = HudiSparkUtil.initSparkSession()
//    var tablePath = basePath +hivedb_Prefix+mongoTable.getMongoDb()+"/"+mongoTable.getTableName()
//    if(mongoTable.getIsPartition()){
//      tablePath=tablePath+"/*"
//    }
//    val roViewDF = spark.read.format("org.apache.hudi").load(tablePath+"/*.parquet")
//    roViewDF.createOrReplaceTempView("hudi_ro_table")
//
//    import spark.implicits._
//    //获取最大提交时间
//    val commits = spark.sql("select max(_hoodie_commit_time) as commitTime from  hudi_ro_table").map(k => k.getString(0)).take(1)
//    val lastCommit = commits(0)
val tablePath = basePath +hivedb_Prefix+mongoTable.getMongoDb()+"/"+mongoTable.getTableName()
    val lastCommit = getMaxCommitTime(tablePath)
    println("lastCommit:"+lastCommit)
    val beginTime=lastCommit -1
    val spark = HudiSparkUtil.initSparkSession()
    import spark.implicits._
    val tripsIncrementalDF = spark.read.format("hudi").
      option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, beginTime).
      load(basePath+hivedb_Prefix+mongoTable.getMongoDb()+"/"+mongoTable.getTableName())
    tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")
    val maxUpdateTime=spark.sql("select max(updatetime) from  hudi_trips_incremental").map(k => k.getString(0)).take(1)(0)
    logger.info("maxUpdateTime:"+maxUpdateTime)
    spark.stop()
     DateTimeUtil.parsedateTime(maxUpdateTime)
  }

  def queryTable(mongoTable:MongoTable): Unit ={
    val spark = HudiSparkUtil.initSparkSession()

    val roViewDF = spark.read.format("org.apache.hudi").parquet(basePath+hivedb_Prefix+mongoTable.getMongoDb()+"/"+mongoTable.getTableName()+"/*.parquet")
    roViewDF.createOrReplaceTempView("hudi_ro_table")
    val result = spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, `_id`,appname,apppackagename,createtime,updatetime  from  hudi_ro_table limit 10")
    result.printSchema()
    result.show(10)

  }

}
