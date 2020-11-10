package com.dpl.dws.common.table.mongo.sdk_pro

import com.dpl.dws.common.table.mongo.MongoTable


/**
 * {
 * "_id" : ObjectId("5dede18a66f043cd0c07fa49"),
 * "appName" : "周转贷",
 * "createTime" : ISODate("2019-11-02T07:58:51.000+0000"),
 * "updateTime" : ISODate("2019-11-02T07:58:51.000+0000"),
 * "type" : NumberInt(1),
 * "source" : "0",
 * "appPackageName" : "com.xxxx.zhouzhuandai"
 * }
 */
class risk_app_summary_result extends MongoTable {
  override val  mongoDb = "sdk_pro"
  override val  table_name = "risk_app_summary_result"

  override val isPartition = false
  override val partitionBy = "createTime"
  override val incrementBy = "updateTime"
  override val table_columns = List[String]()
  override val time_columns = List[String]("createTime","updateTime")




}
