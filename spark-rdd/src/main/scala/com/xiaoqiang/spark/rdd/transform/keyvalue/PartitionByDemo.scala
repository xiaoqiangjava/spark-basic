package com.xiaoqiang.spark.rdd.transform.keyvalue

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * key-value类型的rdd，其实是PairRDDFunctions类型，与RDD是没有直接关系的，但是我们使用的时候还是用RDD，这是因为
 * RDD的伴生对象中有一个隐式转换函数rddToPairRDDFunctions
 * PairRDDFunctions中的方法只能用于key-value类型的RDD，值类型的RDD是没有这类方法的
 */
object PartitionByDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("partition-by"))
    // partitionBy可以按照指定的规则进行重分区,默认是[1, 2], [3, 4], 按照Hash分区之后变成[1, 3], [2, 4]
    // HashPartitioner默认的实现是对key做hashcode之后对分区数取模
    sc.makeRDD(List(1, 2, 3, 4))
      .map((_, 1))
      .partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output")

    sc.stop()
  }
}
