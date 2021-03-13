package com.xiaoqiang.spark.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计值类型的RDD中元素出现的次数
 */
object CountByValue {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("count-by-value-demo"))

    val rdd = sc.makeRDD(List(2, 5, 2, 5, 7, 9, 10, 0))
    // 统计数据出现的次数，把数据当成一个整体，这里的value不是k-v中的value，而是值类型的RDD，所以会把
    // k-v当成一个整体来统计出现的次数
    println(rdd.map(("a", _)).countByValue())
    println(rdd.countByValue())


    sc.stop()
  }
}
