package com.xiaoqiang.spark.rdd.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * aggregateByKey算子：是一个通用的聚合函数，可以指定初始状态值，当遇到rdd中的第一个元素时，函数将与初始值
 * 一起进行计算，该聚合算子可以分别对分区内和分区之间指定不同的聚合函数，可以理解为reduceByKey是aggregateByKey
 * 的一个特例，因为reduceByKey在分区内核分区之间的计算逻辑完全是一致的
 * 注意：
 * aggregateByKey第一元素将会和初始值一起参与计算，所以整个函数的返回值类型由初始值的类型来决定
 * 需要注意的是初始值是作用分区内的，所有的分区都会使用该初始值参与计算
 */
object AggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("aggregate-by"))
    // 创建rdd, 在分区内计算最大值，在分区之间求和
    val rdd = sc.makeRDD(List(1, 3, 5, 7))
      .map(("a", _))
      .aggregateByKey(0)(_.max(_), _+_)

    // 相同的key对应的值求平均值，需要记录两个值，初始值和次数，因此函数的初始值应该是一个tuple
    val avgRDD: RDD[(String, (Int, Int))] = sc.makeRDD(List(("a", 2), ("b", 2), ("a", 4), ("b", 6)))
        .aggregateByKey((0, 0))((init, value) => (init._1 + value, init._2 + 1),
          (total, count) => (total._1 + count._1, total._2 + count._2))

    avgRDD.mapValues({
      case (total, count) => total / count
    }).foreach(println)
    println(rdd.collect().toList)

    println("初始值0：" + sc.makeRDD(List(1, 2, 3, 4, 5, 6))
      .map(("a", _))
      .aggregateByKey(0)(_+_, _+_).collect().toList)

    println("初始值10：" + sc.makeRDD(List(1, 2, 3, 4, 5, 6))
      .map(("a", _))
      .aggregateByKey(10)(_+_, _+_).collect().toList)

    sc.stop()
  }
}
