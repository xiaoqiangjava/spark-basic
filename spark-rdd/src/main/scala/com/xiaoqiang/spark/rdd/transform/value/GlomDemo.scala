package com.xiaoqiang.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * glom算子将一个分区的数据直接转换为相同类型的内存数据进行处理，分区不变，意味着转换之后分区的数量不会变，并行度不变
 * flatMap是将数据打算，glom是将一个分区的数据聚合成Array  RDD[Int] -> RDD[Array[Int]]
 */
object GlomDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("glom-demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 创建rdd
    val rdd = sc.makeRDD(Seq(1, 3, 5, 7), 2)
    // glom算子取出一个分区的全部数据
    val arrRDD: RDD[Array[Int]] = rdd.glom()
    arrRDD.collect().foreach(arr => println(arr.mkString(",")))
    // 对每个分区的数据取出最大值然后求和
    val result = rdd.glom().map(arr => arr.max).sum()

    println(result)

    sc.stop()
  }
}
