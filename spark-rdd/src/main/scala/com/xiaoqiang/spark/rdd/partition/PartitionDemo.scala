package com.xiaoqiang.spark.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark中为了实现分布式计算，引入了分区的概念，分区的数量是在读取数据的时候计算得到的，不同的数据读取方式有'不同
 * 的分区策略，分区跟并行度密切相关，当spark计算完成输出结果时，分区的数量就是输出的结果文件的数量
 * spark中的分区跟并行度密切相关，在资源充足的情况下，分区数量=并行度
 */
object PartitionDemo {
  def main(args: Array[String]): Unit = {
    // 0. 创建执行环境
    val conf = new SparkConf()
    conf.setAppName("partition-demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 1.1 从内存中创建rdd并指定分区数量numSlices, 默认情况下使用的分区数量是通过local[*]获取的，*表示的是当前系统的
    // 可用最大核心数，默认从配置spark.default.parallelism读取
    val rdd: RDD[Int] = sc.makeRDD(Seq(1, 3, 5, 7, 9))
    // 1.2 从文件创建rdd，可以指定最小分区数量minPartition，读取文件使用的Hadoop的FileInputFormat来读取，因此具体的
    // 分区数量是通过FileInputFormat.getSplits()来获取的, 因此实际的分区数量大于等于minPartition
    val fileRDD: RDD[String] = sc.textFile("spark-rdd/src/main/resources/word.txt", 2)

    // -1. 将结果输出到文件可以查看分区的数量
    fileRDD.saveAsTextFile("./output")
  }
}
