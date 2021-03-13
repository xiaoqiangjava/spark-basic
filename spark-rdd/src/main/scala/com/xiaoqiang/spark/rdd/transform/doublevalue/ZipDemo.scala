package com.xiaoqiang.spark.rdd.transform.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * zip算子：拉链操作，将两个rdd对应位置的元素组成一个tuple
 * zip操作的前提是两个rdd每个分区的数量是相同的并且两个分区中的数据量是相同的
 */
object ZipDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("zip-demo"))

    // 创建rdd
    val rdd = sc.makeRDD(List(1, 3, 5, 7))
    val other = sc.makeRDD(List(2, 4, 6, 8))

    println(rdd.zip(other).collect().toList)
    rdd.zip(other).saveAsTextFile("output")

    sc.stop()
  }
}
