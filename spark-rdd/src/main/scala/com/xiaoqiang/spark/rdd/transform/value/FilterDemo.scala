package com.xiaoqiang.spark.rdd.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * filter算子：返回满足表达式的数据集, 将满足表达式的数据保留，不满足表达式的需求舍弃，但是整个过程中
 * 的分区是保持不变的，这就可能导致分区内的数据分布不均匀，出现数据倾斜
 */
object FilterDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("filter-demo").setMaster("local[2]"))
    println(sc.makeRDD(List(1, 2, 3, 4))
      .filter(_ % 2 == 0)
      .collect()
      .toList)
  }
}
