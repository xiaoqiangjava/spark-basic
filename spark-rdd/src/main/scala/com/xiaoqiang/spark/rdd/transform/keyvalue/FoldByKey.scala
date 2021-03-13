package com.xiaoqiang.spark.rdd.transform.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * foldByKey算子：由于aggregateByKey算子中指定的区间内核区间之间的聚合方式可以是一样的，当一样是spark
 * 为我们提供了一个简写的方法，就是foldByKey
 */
object FoldByKey {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("fold-by"))
    sc.makeRDD(List(1, 2, 3, 4, 5, 6))
        .map(("a", _))
        .foldByKey(0)(_+_)
        .foreach(println)

    sc.stop()
  }
}
