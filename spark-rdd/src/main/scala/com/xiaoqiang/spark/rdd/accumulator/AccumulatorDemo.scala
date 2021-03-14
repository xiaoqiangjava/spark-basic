package com.xiaoqiang.spark.rdd.accumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 在spark中，RDD内部定义的函数是在Executor端来执行，RDD外部定义的变量在Driver端执行，如果我们再Driver
 * 端定义一个变量，需要在Executor端修改状态后返回，那么久需要累加器来实现
 * 累加器的作用就是把Executor端的变量信息聚合到Driver端，在Driver程序中定义的变量，在Executor的每个Task
 * 中保留一个变量的副本，每个Task更新副本的之后传给Driver端进行聚合(merge)
 *
 * 累加器是一个分布式的只写变量，只写的意思是不同的Task之间是看不到彼此的累加器的
 *
 * 一般情况下累加器放在行动算子中进行操作防止多加或者少加的情况
 *
 */
object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    // 创建spark核心入口
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("accumulator-demo"))

    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    // 声明一个累加器
    val accumulator = sc.longAccumulator("sum")
    // 如果不使用accumulator的话，这里面变量的值修改之后Driver端是拿不到的，因为foreach函数没有返回值
    rdd.foreach(num => accumulator.add(num))

    // 获取accumulator的值
    println(accumulator.value)

    // 累加器在使用中需要注意：少加、多加问题
    // 如果在transform中调用了累加器，没有执行action，是不会累加的，既少加
    val longAccu = sc.longAccumulator("longAccu")
    val longRDD = rdd.map(longAccu.add(_))

    println("longAccu: " + longAccu.value) // 0, 不执行

    // 多加：多次调用action会多加
    longRDD.collect()
    println(("第一次collect: " + longAccu.value))
    longRDD.collect()
    println(("第二次collect: " + longAccu.value))

    sc.stop()
  }
}
