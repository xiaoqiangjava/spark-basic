package com.xiaoqiang.spark.rdd.cache

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * persist的作用跟cache的作用一样都是持久化数据，其实cache是persist的特例，将数据保存到内存中
 * 可以自己指定持久化的级别
 * persist持久化是不会切断RDD的血缘关系的，当持久化数据丢失之后还会通过血统继续执行计算
 */
object PersistDemo {
  def main(args: Array[String]): Unit = {
    // 创建spark核心入口
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("persist-demo"))
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

    // persist默认的持久化是只保存到内存中，可以指定不同的级别，需要注意的是如果级别后面带有_2的字样表示副本的数量
    oneCount.persist(StorageLevel.MEMORY_AND_DISK)

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
