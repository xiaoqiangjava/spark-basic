package com.xiaoqiang.spark.rdd.accumulator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 广播变量：分布式的只读变量
 * spark中闭包数据都是以Task为单位发送的Executor的，这样的话每个Task中都保存一份闭包数据，那么一个Executor
 * 中就会存在大量的重复数据，占用内存，如果我们将闭包中的数据保存一份，放到Executor就能达到所有的Task共享
 * 数据的目的。Spark中通过broadcast来实现将闭包中的数据保存到Executor内存中，共享数据
 */
object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("broadcast-demo"))
    // 使用broadcast共享变量
    val map = Map(("a", 1), ("b", 3), ("c", 5))
    // 将map包装成共享变量
    val bc: Broadcast[Map[String, Int]] = sc.broadcast(map)

    val rdd = sc.makeRDD(List(("a", 2), ("b", 4), ("c", 6)))
    // 使用map来简化join操作实现(a, (1, 2)), (b, (3, 4))效果
    rdd.map({
      case (word, count) => {
        // 这里的map是闭包外部的变量，spark执行时会将map以Task为单位保存副本，为了使用共享变量，引入broadcast
        (word, (count, bc.value.getOrElse(word, 0)))
      }
    }).foreach(println)

    sc.stop()
  }
}
