package com.xiaoqiang.spark.rdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 * 统计一个手机用户在哪两个基站停留的时间最长
 * 数据格式：手机号,时间,基站,操作类型（1-进入，0-退出）
 * 15095328593,20190504133500,1ES9876623LJHGS,1
 * 15095328593,20190504143500,1ES9876623LJHGS,0
 */
object StatisticsDuration {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName(getClass.getName).setMaster("yarn")
        val sc = new SparkContext(conf)
        // 1. 从参数列表中获取文件输入和输出路径
        val input = args(0)
        val output = args(1)
        // 2. 将手机号跟基站分为一组，时间跟时间类型分为一组
        val pairData = sc.textFile(input).map(str => {
            val strArray = str.split(",")
            ((strArray(0), strArray(2)), (strArray(1), strArray(3).toInt))
        })
        val totalDuration = pairData.groupByKey().mapValues(temp => {
            val iterator: Iterator[Iterable[(String, Int)]] = temp.grouped(2).filter(_.size == 2)
            val timeRDD: Iterator[Iterable[Long]] = iterator.map(_.map(x => DateUtil.calDutarion(x._1)))
            // 将时间转换成时间戳
            val duration = timeRDD.map(_.reduce(_ - _)).sum
            duration
        })
        totalDuration.saveAsTextFile(output)
    }
}
