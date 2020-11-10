package com.dpl.dws.hudi


import java.util.{HashMap, Map}

import com.dpl.dws.common.constant.Constant
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.HoodieIndex

object HudiOptions {

  def getQuickstartWriteConfigs: Map[String, String] = {
    val demoConfigs = new HashMap[String, String]
    // 设置jdbc 连接同步 hive
    demoConfigs.put(DataSourceWriteOptions.HIVE_URL_OPT_KEY, Constant.hive_url)
    // 并行度参数设置
    demoConfigs.put("hoodie.insert.shuffle.parallelism", "2")
    demoConfigs.put("hoodie.upsert.shuffle.parallelism", "2")
   // demoConfigs.put(DataSourceWriteOptions.HIVE_SUPPORT_TIMESTAMP, "true")//支持时间戳
      // 设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
    demoConfigs.put(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
    demoConfigs
  }

}
