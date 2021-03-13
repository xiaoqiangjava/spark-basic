package com.xiaoqiang.spark.rdd.transform.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 对两个rdd中的数据求交集，求并集之后分区的数量不会发生变化
 */
object IntersectionDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("intersection-demo"))
    // 创建rdd
    val rdd = sc.makeRDD(List(0, 4, 5, 7))
    val other = sc.makeRDD(List(1, 4, 6, 8))

    // 求交集
    val result = rdd.intersection(other)
    result.saveAsTextFile("output")

    sc.stop()
  }
}
