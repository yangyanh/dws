package com.dpl.dws.common.table.mongo

class MongoTable {

  protected val mongoDb = ""
  protected val table_name = ""
  protected val isPartition = false
  protected val partitionBy = "createTime"
  protected val incrementBy = "updateTime"
  protected val table_columns = List[String]()
  protected val time_columns = List[String]()

  def getMongoDb():String={
    mongoDb
  }

  def getTableName():String={
    table_name
  }

  def getIsPartition():Boolean={
    isPartition
  }

  def getIncrementBy():String={
    incrementBy
  }

  def getPartitionBy():String={
    partitionBy
  }

  def gettableColumns():List[String]={
    table_columns
  }

  def getTimeColumns():List[String]={
    time_columns
  }


  override def toString = s"MongoTable($table_name, $isPartition, $partitionBy, $table_columns, $time_columns)"
}
