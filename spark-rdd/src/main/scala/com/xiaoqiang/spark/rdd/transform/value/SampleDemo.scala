package com.xiaoqiang.spark.rdd.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * sample算子：表示抽取数据，有三个参数，第一个参数withReplacement表示数据抽取之后是否还要放回去
 * 放回去表示数据可能会被重复抽取，第二个参数fraction表示每条数据被抽取的概率，第三个数据是随机数种子
 * sample主要用在校验是否有数据倾斜，spark读取数据的时候，数据是均匀分布的，但是经过前面的groupBy这种
 * 操作之后，就会触发shuffle操作，极有可能导致数据倾斜，通过sample可以找出数据倾斜的k，然后修改分区策略
 */
object SampleDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("sample-demo"))
    // 这里需要注意，如果随机数的种子设定了固定值，那么每次抽样抽取的数据都是一样的，默认是系统时间戳，每次都一样
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
      .sample(true, 0.4, 10)

    println(rdd.collect().toList)
  }
}
