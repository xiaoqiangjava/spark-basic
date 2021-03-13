package com.xiaoqiang.spark.rdd.transform.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * reduceByKey算子：相同key的数据进行value值的聚合
 * Scala中的聚合都是两两聚合的，所以spark中的聚合也是两两来聚合，因此如果对应的key中只有一个元素
 * 的话，这个key是不参与计算的, reduceByKey操作存在shuffle操作
 */
object ReduceByKeyDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("reduce-by-key"))

    // 创建rdd
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 4), ("a", 3)))
    rdd.reduceByKey((x, y) => {
      println(s"x=${x}, y=${y}")
      x + y
    }).foreach(println)

    sc.stop()
  }
}
