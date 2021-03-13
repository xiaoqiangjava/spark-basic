package com.xiaoqiang.spark.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * foreach是个action，他会将数据进行循环输出，需要注意的是RDD的foreach方法时在Executor端直接执行的
 * 因此如果在本地将任务提交到yarn，那么是看不到输出信息的，如果想要在driver端看到输出信息，可以通过collect
 * 方法来收集数据到driver端，然后打印
 */
object ForeachDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("foreach-demo"))

    val rdd = sc.makeRDD(List(1, 2, 3, 5, 7, 9, 10))
    // 打印血缘关系
    println(rdd.toDebugString)
    // 打印依赖关系
    println(rdd.dependencies)

    // 这里的foreach方法时Scala中集合的foreach
    rdd.collect().foreach(println)
    println("*********")

    // rdd的foreach
    rdd.foreach(println)

    sc.stop()
  }
}
