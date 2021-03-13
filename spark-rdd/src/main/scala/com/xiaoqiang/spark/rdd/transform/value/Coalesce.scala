package com.xiaoqiang.spark.rdd.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * coalesce用于缩减分区, 但是coalesce操作默认情况下是不会打算数据重新组合的，也就是说他不会将一个分区里面
 * 的数据进行拆分，然后与其他分区的数据组合，只会整体将这个分区的数据放到一个分区中去
 * 这种情况下的缩减分区可能会导致数据的倾斜，因此可以将数据打乱之后再重新分区
 * coalesce(true) 指定参数为true之后，就会触发shuffle操作，将数据打乱之后重新分区
 * 注意：
 * coalesce(true)发生shuffle操作之后，就算指定的分区数没有发生变化，里面的数据也会发生变化
 * coalesce默认情况下作只能缩减分区，不能用于增加分区，要增加分区时，就需要指定为shuffle操作
 * 为了记忆使用方便，spark帮我们封装了一个repartition操作因此约定了coalesce操作用来缩减
 * 分区，repartition用于增加分区
 */
object Coalesce {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("distinct-demo"))

    // 创建rdd
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    // 缩减分区之前有3个分区，分别存放数据[1, 2], [3, 4], [5, 6]
    rdd.saveAsTextFile("output")

    // 缩减分区之后由于不会将分区数据打算，所以[3, 4]分区中的数据被整体放在了[5, 6]所在的分区
    val coalRDD = rdd.coalesce(2)
    coalRDD.saveAsTextFile("output1")

    // shuffle之后重分区
    val shuffleRDD = rdd.coalesce(2, true)
    shuffleRDD.saveAsTextFile("output3")

    // 增加分区, 需要指定shuffle操作为true
    val addRDD = rdd.coalesce(4, true)
    addRDD.saveAsTextFile("output4")

    val repartitionRDD = rdd.repartition(4)
    repartitionRDD.saveAsTextFile("output5")
    sc.stop()
  }
}
