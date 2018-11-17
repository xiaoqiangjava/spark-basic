package com.xiaoqiang.spark.streaming

import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path, RawLocalFileSystem}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingDemo {
    def main(args: Array[String]): Unit = {
        // Create a local StreamingContext with two execution threads, and a batch interval of 1 second
        val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(conf, Seconds(5))
        // Create a DStream that represents streaming data from a TCP source
        // val lines = ssc.socketTextStream("localhost", 9999)
        val fs = new RawLocalFileSystem

        val lines = ssc.textFileStream("E:\\wordcount")
        // split each line into words
        val wordRdd = lines.flatMap(_.split(" "))
        // count each word in each batch
        val countRdd = wordRdd.map((_, 1))
        val wordCount = countRdd.reduceByKey((_ + _))
        wordCount.print()
        println("********************************")

        // Start the computation
        ssc.start()
        // Wait for the computation to terminate
        ssc.awaitTermination()
    }

}
