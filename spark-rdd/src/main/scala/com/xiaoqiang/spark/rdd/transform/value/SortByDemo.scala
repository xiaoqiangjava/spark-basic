package com.xiaoqiang.spark.rdd.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * sortBy算子，用于排序，默认情况下sortBy操作分区是不变的，但是该操作跟reduceBy操作一样会触发
 * shuffle操作，因此shuffle之后的分区中数据是有序的
 */
object SortByDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("sort-by"))
    // 创建rdd
    val rdd = sc.makeRDD(List(6, 3, 4, 1, 2))
    val ascRDD = rdd.sortBy(num => num)
    ascRDD.saveAsTextFile("output")

    // 排序之后分区不会发生变化, 可以指定第二个参数控制升序还是降序，也可以取反操作来控制
    val descRDD = rdd.sortBy(num => num, false)
    descRDD.saveAsTextFile("output1")

    rdd.sortBy(num => -num).saveAsTextFile("output2")

    sc.stop()
  }
}
