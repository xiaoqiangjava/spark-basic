package com.xiaoqiang.spark.rdd.transform.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * join算子：将两个add按照相同的key进行关联，相同key的value形成一个tuple
 * 如果两个数据源中key没有匹配上，那么数据不会出现在结果中
 * 如果两个数据源中key有多个相同的，会一次匹配，可能会出现笛卡尔低积，数据会几何形增长
 */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("join-demo"))
    // join操作: (a, (2, 4)), (b, (2, 6))
    // 如果rdd的数据中多了一个(a, 8), 那么返回结果为：(a, (2, 4)), (b, (2, 6)), (a, (2, 8))
    val rdd = sc.makeRDD(List(("a", 2), ("b", 2)))
    val other = sc.makeRDD(List(("a", 4), ("b", 6)))
    rdd.join(other).foreach(println)

    sc.stop()
  }
}
