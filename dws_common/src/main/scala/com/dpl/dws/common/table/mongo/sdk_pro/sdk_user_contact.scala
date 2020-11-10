package com.dpl.dws.common.table.mongo.sdk_pro

import com.dpl.dws.common.table.mongo.MongoTable

class sdk_user_contact extends MongoTable{

  override val  mongoDb = "sdk_pro"
  override val  table_name = "sdk_user_contact"
  override val isPartition = true
  override val partitionBy = "createTime"
  override val incrementBy = "updateTime"
  override val table_columns = List[String]("createTime","deleted","mobile","name","updateTime","userId","reportTime")
  override val time_columns = List[String]("createTime","updateTime","reportTime")

}
