package com.xiaoqiang.spark.rdd.transform.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * subtract求差集，求出当前rdd有，另一个rdd没有的数据
 * 差集不会改变分区，但由于数据只来源于一个rdd，因此会触发shuffle操作
 */
object SubtractDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("subtract-demo"))
    // 创建rdd
    val rdd = sc.makeRDD(List(1, 3, 4, 5, 7, 11, 34, 22, 55))
    val other = sc.makeRDD(List(2, 4, 6, 8))
    rdd.saveAsTextFile("output1")

    println(rdd.subtract(other).collect().toList)
    rdd.subtract(other).saveAsTextFile("output")

    sc.stop()
  }
}
