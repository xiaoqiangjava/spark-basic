package com.xiaoqiang.spark.rdd.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * distinct算子去重：
 * 底层实现上是通过reduceByKey结合map来实现的，即通过K聚合之后，直接丢弃掉value
 */
object DistinctDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("distinct-demo"))
    // 创建rdd
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))
    // 1. 自己通过reduceByKey来实现去重功能
    val disRDD = rdd.map((_, None))
      .reduceByKey((x, _) => x)
      .map(_._1)

    println(disRDD.collect().toList)

    // 2. 通过distinct函数来实现, 由于reduceByKey是一个shuffle操作，所以会导致数据量的变化，我们可以重新
    // 指定分区，在做reduceByKey的时候对数据进行重分区
    println(rdd.distinct().collect().toList)
    rdd.saveAsTextFile("output1")
    val repartitionRDD = rdd.distinct(4)

    repartitionRDD.saveAsTextFile("output")
  }
}
