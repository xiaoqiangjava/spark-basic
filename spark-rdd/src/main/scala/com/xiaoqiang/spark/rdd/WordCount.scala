package com.xiaoqiang.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setMaster("local")
        conf.setAppName("WordCount")
        // 创建spark核心入口
        val sc = new SparkContext(conf)
        // 读取文件生成RDD
        val file = sc.textFile("E:\\hello.txt")
        // 将文件中的每一行使用,分隔成一个list再展开
        val wordRdd = file.flatMap(_.split(","))
        // 每一个单词出现1次
        val oneCount = wordRdd.map(word=>(word.trim, 1))
        // 计算每个单词出现的总次数
        val wordCount = oneCount.reduceByKey((_ + _))
        // 将单词的统计信息打印
        println("统计每个单词出现的总次数: ")
        wordCount.saveAsTextFile("E:\\success")
        wordCount.foreach(count => println(count))
        println("计算完毕")
        sc.stop()
    }

}
