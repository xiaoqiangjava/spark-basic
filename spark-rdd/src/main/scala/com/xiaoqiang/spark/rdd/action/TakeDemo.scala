package com.xiaoqiang.spark.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * take用于取出指定个数的数据
 * takeOrdered: 取出排序后的指定个数的数据,只能取最小的指定个数
 */
object TakeDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("reduce-demo"))

    val rdd = sc.makeRDD(List(91, 2, 5, 7, 9))
    println(rdd.take(3).mkString(","))

    // 取最小的3个数
    println(rdd.takeOrdered(3).mkString(","))

    // 取最大的是3个数
    println("top-top: " + rdd.top(3).mkString(","))
    println("top-takeOrdered: " + rdd.takeOrdered(3)(Ordering.by[Int, Int](x => -x)).mkString(","))

    sc.stop()
  }
}
