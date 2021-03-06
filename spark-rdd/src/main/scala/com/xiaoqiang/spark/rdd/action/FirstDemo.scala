package com.xiaoqiang.spark.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * action算子可以出发作业的提交, 行动算子返回的是数据，不在是RDD
 * first用于取出数据的第一个值
 */
object FirstDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("reduce-demo"))

    val rdd = sc.makeRDD(List(1, 3, 5, 7, 9))
    println(rdd.first())

    sc.stop()
  }
}
