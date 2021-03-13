package com.xiaoqiang.spark.rdd.transform.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * union求两个rdd的并集, 需要注意的是求并集之后，分区的数量也是求并集
 */
object UnionDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("union-demo"))
    // 创建rdd
    val rdd = sc.makeRDD(List(1, 3, 4, 5, 7))
    val other = sc.makeRDD(List(2, 3, 4, 6, 8))

    val result = rdd.union(other)
    println(result.collect().toList)

    result.saveAsTextFile("output")

    sc.stop()
  }
}
