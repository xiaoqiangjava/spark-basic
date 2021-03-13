package com.xiaoqiang.spark.rdd.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupBy算子，按照指定的k进行分组，分区之后得到的是RDD[(K, Iterable[T])]
 */
object GroupByDemo {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("groupBy-demo"))
    // 2. 创建RDD
    val rdd = sc.makeRDD(Seq(1, 2, 3, 4))
    // 3. 使用groupBy按照奇偶数进行分组, 需要注意的是分组和分区没有关系
    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(_%2)
    println(groupRDD.collect().toList)

    // 4. 按照首字母分组，所有的分组都在一个K，输出之后还是两个分区, 但是只有一个分区有数据
    sc.makeRDD(Seq("Hello", "Hadoop", "Help", "Hi"))
        .groupBy(_.charAt(0))
        .saveAsTextFile("output1")

    sc.makeRDD(Seq("Hello", "Hadoop", "Help", "Hi"))
        .saveAsTextFile("output2")

    groupRDD.saveAsTextFile("output")
  }
}
