package com.xiaoqiang.spark.rdd.accumulator

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 自定义累加器，有时候可以避免shuffle操作，进而提高程序运行的性能
 * 自定义的累加器需要注册到spark中，spark为我们提供了register方法用来注册累加器
 */
object AccumulatorCustomer {
  def main(args: Array[String]): Unit = {
    // 创建spark核心入口
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("accumulator-customer"))

    // 注册累加器
    val countAccumulator = new WordCountAccumulator
    sc.register(countAccumulator, "wc-accumulator")

    val rdd = sc.makeRDD(List("hello world", "hello scala"))

    // 使用累加器
    rdd.flatMap(_.split(" ")).foreach(countAccumulator.add)

    // 获取累加器的值
    println(countAccumulator.value)

    sc.stop()

  }
}

// 自定义累加器，需要实现AccumulatorV2
class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {
  val accumulator: mutable.Map[String, Int] = mutable.Map[String, Int]()

  // 是否是初始化状态
  override def isZero: Boolean = accumulator.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new WordCountAccumulator

  // 情况累加器
  override def reset(): Unit = accumulator.clear

  // 累加器中新增值
  override def add(v: String): Unit = {
    accumulator.update(v, accumulator.getOrElse(v, 0) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    val map = other.value
    map.foreach({
      case (word, count) => {
        accumulator.update(word, accumulator.getOrElse(word, 0) + count)
      }
    })
  }

  // value方法直接返回map
  override def value: mutable.Map[String, Int] = accumulator
}