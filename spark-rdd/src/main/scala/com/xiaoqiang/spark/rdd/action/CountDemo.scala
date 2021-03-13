package com.xiaoqiang.spark.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * count求数据总数
 */
object CountDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("reduce-demo"))

    val rdd = sc.makeRDD(List(91, 2, 5, 7, 9, 10))
    println(rdd.count())


    sc.stop()
  }
}
