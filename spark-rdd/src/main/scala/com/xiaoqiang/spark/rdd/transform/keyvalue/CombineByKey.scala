package com.xiaoqiang.spark.rdd.transform.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * combineByKey算子：也是用来实现聚合的函数，与aggregateByKey不同的是aggregateByKey需要指定初始值参与计算，
 * 而combineByKey是通过将第一个值转换成一个固定的值进行计算，后面的两个参数都是相同的含义，一个是用来在分区间进行
 * 计算，另一个是在分区之间进行计算
 */
object CombineByKey {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("combine-by"))
    // 通过combineByKey实现聚合：相同的key求平均值, 这里的第一个参数表示相同k第一次的处理逻辑
    sc.makeRDD(List(("a", 2), ("b", 2), ("a", 4), ("b", 6)))
      .combineByKey((_, 1), (c: (Int, Int), v) => (c._1 + v, c._2 + 1),
        (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c1._2 + c2._2))
      .foreach(println)

    sc.stop()
  }
}
