package com.xiaoqiang.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * map算子中一个分区中的计算是一行数据一行数据来计算的，也就是说读取一行数据之后就会把所有的计算都做一遍，然后读取第二个数据
 * 整个计算过程中消耗的内存与数据量的大小无关
 * 但是一行一行的处理数据效率不高，因此应该考虑缓冲区，那么spark中也为我们提供了相应的方法: mapPartitions
 */
object MapDemo {
  def main(args: Array[String]): Unit = {
    // 0. 获取执行环境
    val conf = new SparkConf().setMaster("local[2]").setAppName("map-demo")
    val sc = new SparkContext(conf)
    // 1. map算子，主要实现数据类型的转换，将读取到的数据转换为我们想要处理的数据类型
    // RDD是不可变数据类型, 使用算子转换之后就会产生新的RDD，有多个算子时就会形成依赖关系
    val rdd = sc.makeRDD(Seq(1, 2, 3, 4))
    val mapRDD: RDD[Int] = rdd.map(_ * 1)

    println(s"${Thread.currentThread().getName}: 这里在driver端执行，所以只执行一次")
    // 分区中的输出时顺序执行的，不同的分区并行执行，因此如果这里分区数设置为2，那么1>2, 3>4，但是1，3和2，4的顺序不定
    mapRDD.foreach(row => {
      println(s"${Thread.currentThread().getName}: foreach在Executor中执行，是并行的")
      println(row)
    })

    // 2. mapPartitions算子：相关与map算子带有缓冲区，不再是一个个来处理数据，而是一个分区来处理, 由于是整个分区的内容
    // 那么接受的参数不再是一个元素，而是一个Iterator
    // 需要注意的是mapPartitions缓存了数据，因此存在内存中的引用，在数据量较大时容易导致内存溢出，但如果map中存在对数据库或者
    // 文件的操作，那么map效率会很低，使用mapPartitions会大幅提高性能
    // 为了更加高效的使用mapPartitions，可以自定义一个迭代器，防止将数据一次性加载到内存导致内存溢出
    val mapPar = rdd.mapPartitions(rows => {
      println("整个分区来处理：" + rows.toList)
      rows.toArray.toIterator
    })

    // 当需要分区index时，可以使用mapPartitionsWithIndex
    // 获取每个分区的最大值
    val mpRDD = rdd.mapPartitions(iter => Iterator(iter.max))
    println(mpRDD.collect().toList)

    // 3. 使用自定义Iterator防止mapPartitions方法导致内存溢出, 将RDD中的数乘2返回
    val iterRDD = rdd.mapPartitions(new CustomIterator(_))
    println(iterRDD.collect().toList)

    // 4. 使用mapPartitionsWithIndex算子获取分区号
    println(rdd.mapPartitionsWithIndex((idx, iter) => Iterator((idx, iter.max))).collect().toList)
    mapPar.collect()
    // 关闭执行环境
    sc.stop()
  }
}

class CustomIterator(iter: Iterator[Int]) extends Iterator[Int] {
  override def hasNext: Boolean = iter.hasNext

  override def next(): Int = iter.next() * 2
}
