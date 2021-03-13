package com.xiaoqiang.spark.rdd.transform.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupByKey算子：将数据源中相同key的数据分在一个组，形成一个对偶元组，元组的第一元素时key，第二个元素时value集合
 * 与RDD中groupBy的区别是RDD中生成的value是(K, V)的集合，而groupByKey中时value值的集合，不包含key
 * groupByKey会导致数据打乱重组，存在shuffle操作，而且在spark中，shuffle操作必须落盘处理，不能在内存中数据等待，不然
 * 在运行过程中会导致内存溢出，所以shuffle操作的效率是非常低的
 * groupByKey之后可以通过map操作去聚合数据，这样就实现了reduceByKey的功能，但是这里就有一个问题，shuffle操作的落盘
 * 效率是非常低的，如果只想做聚合操作的话，那么在落盘之前就可以对数据进行聚合，这样的话就会减少IO操作次数，提高效率，因此
 * 在需要做聚合操作的时候还是要使用reduceByKey来实现，reduceByKey在落盘之前在分区内的数据进行了预聚合(combine)
 */
object GroupByKeyDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("group-by-key"))
    // 创建rdd
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val groupRDD = rdd.groupBy(_._1)
    val groupByKeyRDD = rdd.groupByKey()

    // 结果：(b,CompactBuffer((b,2)))
    groupRDD.collect().foreach(println)

    // 结果：(b,CompactBuffer(2))
    groupByKeyRDD.collect().foreach(println)

    sc.stop()
  }
}
