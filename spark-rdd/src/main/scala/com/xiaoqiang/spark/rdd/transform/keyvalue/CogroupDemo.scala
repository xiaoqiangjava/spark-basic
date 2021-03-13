package com.xiaoqiang.spark.rdd.transform.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * cogroup算子：可以理解我connect + group, 对两个rdd中的相同key的数据先进行分组，然后拼接到一个集合中
 * ("a", 1), ("b", 2), ("b", 3)和("a", 4), ("a", 6), ("b", 3)
 *  结果：
 *  ("a", [1], [4, 6])
 *  ("b", [2, 3], [3])
 */
object CogroupDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("co-group"))
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("b", 3)))
    val other = sc.makeRDD(List(("a", 4), ("a", 6), ("b", 3)))
    rdd.cogroup(other).foreach(println)

    sc.stop()
  }
}
