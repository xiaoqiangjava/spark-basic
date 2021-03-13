package com.xiaoqiang.spark.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * fold：也是一个聚合行动，与aggregate的区别就是当分区间和分区内的计算逻辑相同是使用fold
 * fold的初始值同样也是作用于分区间和分区内的
 * 这个跟foleByKey跟aggregateByKey的区别是相同的，一一对应
 */
object FoldDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[3]").setAppName("fold-demo"))

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9))
    val sum = rdd.fold(0)(_+_)
    println(sum)
    println(rdd.fold(10)(_+_))

    sc.stop()
  }
}
