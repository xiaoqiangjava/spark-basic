package com.xiaoqiang.spark.rdd.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap算子作用跟map相似，只是flatMap的中间结果应该返回一个集合，flat的作用就是将中间结果继续展开
 */
object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("flatmap-demo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 创建rdd
    val rdd = sc.makeRDD(Seq(List(1, 2), List(3, 4), List(5, 6)))
    // flatMap
    println(rdd.flatMap(list => list).collect().toList)
    sc.stop()
  }
}
