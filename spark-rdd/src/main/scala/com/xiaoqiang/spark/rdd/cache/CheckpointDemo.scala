package com.xiaoqiang.spark.rdd.cache

import org.apache.spark.{SparkConf, SparkContext}

/**
 * checkpoint检查点也是用来做缓存，不同的是在做检查点之前需要指定数据保存的目录，因为检查点保存之后会切断
 * RDD的血统，重新基于检查点建立血统，相当于改变了数据源，如果数据丢失是没办法恢复的, 而且还需要注意的是checkpoint操作会立即出发作业的执行，也就是说
 * checkpoint会多执行一次计算，为了提高性能，一般会在persist之前先进行一次cache，就会减少一次计算
 * persist持久化是不会切断RDD的血缘关系的，当持久化数据丢失之后还会通过血统继续执行计算
 */
object CheckpointDemo {
  def main(args: Array[String]): Unit = {
    // 创建spark核心入口
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("persist-demo"))
    sc.setCheckpointDir("spark-rdd/src/main/resources/")
    // 读取文件生成RDD
    val file = sc.textFile("spark-rdd/src/main/resources/word.txt")
    // 将文件中的每一行使用,分隔成一个list再展开
    val wordRdd = file.flatMap(_.split(","))
    // 每一个单词出现1次
    val oneCount = wordRdd.map(word=>{
      // 如果这里的输出在第一次执行结束之后还打印了，那么说明是从开始计算
      println("************************")
      (word.trim, 1)
    })
    // checkpoint之前的血统
    println(oneCount.dependencies)
    println(oneCount.toDebugString)

    // checkpoint之前cache，会减少一次计算，因为checkPoint会立即出发一次计算，后面的action又会触发一次计算
    // 从后面的第二次开始就不会再触发checkpoint之前的计算，这是因为checkpoint操作是在作业提交之后立即出发的
    oneCount.cache()
    // checkpoint将数据保存到磁盘
    oneCount.checkpoint()

    // 计算每个单词出现的总次数
    val wordCount = oneCount.reduceByKey((_ + _))
    wordCount.collect().foreach(println)
    println("第一次执行结束===============================")
    // checkpoint后血统会切断
    println(oneCount.dependencies)
    println(oneCount.toDebugString)

    // 复用对象
    oneCount.groupByKey().collect().foreach(println)
    println("第二次执行结束===============================")

    sc.stop()
  }
}
