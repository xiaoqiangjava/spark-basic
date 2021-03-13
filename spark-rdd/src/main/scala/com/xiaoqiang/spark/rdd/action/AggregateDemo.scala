package com.xiaoqiang.spark.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * aggregate: 是一个行动算子，指定初始值，分区内计算，分区之间计算规则
 * 需要注意的是这个初始值不只是作用于分区内第一个元素，他还作用于分区间的聚合，即如果有两个分区，那么
 * 两个分区内各加一次，最后分区之间聚合再加一次
 * 之前将的算子aggregateByKey是只在分区内起作用，分区间不参与计算
 */
object AggregateDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("aggregate-demo"))

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    val sum = rdd.aggregate(0)(_+_, _+_) // 55
    val total = rdd.aggregate(10)(_+_, _+_) // 85

    println(sum, total)
  }
}
