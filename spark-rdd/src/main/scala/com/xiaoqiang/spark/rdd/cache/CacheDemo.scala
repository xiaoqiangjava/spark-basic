package com.xiaoqiang.spark.rdd.cache

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 由于spark中的RDD是不保存数据的，只保存了依赖以及血统关系，那么当一个RDD需要重复使用时，如果
 * 我们只是复用了一个对象，spark执行过程中还是会从开操作一遍所有的算子，为了提高性能引入了持久化操作
 * cache操作会在血统中增加新的依赖
 */
object CacheDemo {
  def main(args: Array[String]): Unit = {
    // 创建spark核心入口
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("cache-demo"))
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

    // 使用cache持久化之后，已经计算过一次的数据会保存到内存或者磁盘，不会再次计算
    oneCount.cache()

    // 计算每个单词出现的总次数
    val wordCount = oneCount.reduceByKey((_ + _))
    wordCount.collect().foreach(println)
    println("第一次执行结束===============================")

    // 复用对象
    oneCount.groupByKey().collect().foreach(println)
    println("第二次执行结束===============================")

    sc.stop()
  }
}
