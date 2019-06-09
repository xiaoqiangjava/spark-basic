package com.xiaoqiang.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingDemo {
    def main(args: Array[String]): Unit = {
        // Create a local StreamingContext with two execution threads, and a batch interval of 1 second
        val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.sparkContext.setCheckpointDir("d:/checkpoint")
        // Create a DStream that represents streaming data from a TCP source
        // val lines = ssc.socketTextStream("localhost", 9999)

        val lines = ssc.textFileStream("D:\\wordcount")
        // split each line into words
        val dStream = lines.flatMap(_.split(" "))
        // count each word in each batch
        val countDS = dStream.map((_, 1))
        countDS.cache()
        // updateStateByKey方法会将前面计算得到的结果与当前的结果相加
        val wordCount = countDS.updateStateByKey((vs: Seq[Int], state: Option[Int]) => Some(vs.sum + state.getOrElse(0)), ssc.sparkContext.defaultParallelism)
//        wordCount.print()
        println("********************************")

        // 还可以利用窗口函数来操作, 不同的是窗口操作只统计当前窗口中的所有值，当窗口滚动到下一个窗口时，不会再统计上一个窗口的值，可以通过设置窗口的滚动间隔来控制
        val winOper = countDS.reduceByKeyAndWindow((v: Int, v1: Int) => v + v1, Seconds(30), Seconds(25), ssc.sparkContext.defaultParallelism)
        winOper.print()


        // Start the computation
        ssc.start()
        // Wait for the computation to terminate
        ssc.awaitTermination()
    }

}
