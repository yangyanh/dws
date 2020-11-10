package com.dpl.dws.mongo.utils

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{DeleteManyModel, DeleteOneModel, InsertOneModel, ReplaceOneModel, UpdateOneModel}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.rdd.RDD
import org.bson.Document

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object MongoUtils {

  val DefaultMaxBatchSize = 100000

  def insertSave[D: ClassTag](rdd: RDD[InsertOneModel[Document]]): Unit = insertSave(rdd, WriteConfig(rdd.sparkContext))
  def insertSave[D: ClassTag](rdd: RDD[InsertOneModel[D]], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[D] =>
        iter.grouped(DefaultMaxBatchSize).foreach(batch => collection.bulkWrite(batch.toList.asJava))
      })
    })
  }


  def updateSave[D: ClassTag](rdd: RDD[UpdateOneModel[Document]]): Unit = updateSave(rdd, WriteConfig(rdd.sparkContext))
  def updateSave[D: ClassTag](rdd: RDD[UpdateOneModel[D]], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[D] =>
        iter.grouped(DefaultMaxBatchSize).foreach(batch => collection.bulkWrite(batch.toList.asJava))
      })
    })
  }
  def upsertSave[D: ClassTag](rdd: RDD[ReplaceOneModel[Document]]): Unit = upsertSave(rdd, WriteConfig(rdd.sparkContext))
  def upsertSave[D: ClassTag](rdd: RDD[ReplaceOneModel[D]], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[D] =>
        iter.grouped(DefaultMaxBatchSize).foreach(batch => collection.bulkWrite(batch.toList.asJava))
      })
    })
  }

  def deleteSave[D: ClassTag](rdd: RDD[DeleteOneModel[Document]]): Unit = deleteSave(rdd, WriteConfig(rdd.sparkContext))
  def deleteSave[D: ClassTag](rdd: RDD[DeleteOneModel[D]], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[D] =>
        iter.grouped(DefaultMaxBatchSize).foreach(batch => collection.bulkWrite(batch.toList.asJava))
      })
    })
  }


  def deleteManySave[D: ClassTag](rdd: RDD[DeleteManyModel[Document]]): Unit = deleteManySave(rdd, WriteConfig(rdd.sparkContext))
  def deleteManySave[D: ClassTag](rdd: RDD[DeleteManyModel[D]], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[D] =>
        iter.grouped(DefaultMaxBatchSize).foreach(batch => collection.bulkWrite(batch.toList.asJava))
      })
    })
  }

}
