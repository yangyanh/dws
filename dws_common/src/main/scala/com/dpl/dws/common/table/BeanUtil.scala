package com.dpl.dws.common.table

import com.dpl.dws.common.table.mongo.MongoTable

object BeanUtil {
  def main(args: Array[String]): Unit = {
    val mongoDb = "sdk_pro"
    val table_name = "risk_app_summary_result"
//    val table_name = "sdk_user_contact"
    val table = getMongoTable(mongoDb,table_name)
    if(table==null){
      println("table "+table_name + " not found!")
      return
    }
    println(table.toString())
    println(table.gettableColumns().isEmpty)
  }

  def getMongoTable(mongoDb:String,table_name:String): MongoTable ={
    try {
      Class.forName("com.dpl.dws.common.table.mongo."+mongoDb+"."+table_name).newInstance().asInstanceOf[MongoTable]
    }catch {
      case e:ClassNotFoundException =>{
        println("table "+table_name + " not found!")
        null
      }
      case e:Exception =>{
        e.printStackTrace()
        null
      }
    }
  }

}
