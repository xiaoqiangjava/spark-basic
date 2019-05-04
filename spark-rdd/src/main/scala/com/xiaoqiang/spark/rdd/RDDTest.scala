package com.xiaoqiang.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/*
 * 常用算子练习
 */
object RDDTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("RDDTest").setMaster("local")
        val sc = new SparkContext(conf)
        // 0. 通过并行化生成rdd
        val rdd1 = sc.parallelize(List(5, 6, 7, 4, 2, 3, 1, 9, 8))
        // 1. 对rdd1中的所有元素乘以2，然后在排序
        val rdd2 = rdd1.map(_ * 2).sortBy(x => x)
        println(rdd2.collect().toBuffer)
        // 2. 过滤出大于等于十的元素
        val rdd3 = rdd2.filter(_ >= 10)
        println(rdd3.collect().toBuffer)
        val rdd4 = sc.parallelize(List("a b c d", "e f g h", "i j k l"))
        // 3. 将rdd4中的元素先切分再压平
        val rdd5 = rdd4.flatMap(_.split(" "))
        println(rdd5.collect().toBuffer)
        val rdd6 = sc.parallelize(List(List("a b c d"), List("e f g h"), List("i j k l")))
        val rdd7 = rdd6.flatMap(_.flatMap(_.split(" ")))
        println(rdd7.collect().toBuffer)
        // 4. 求并集
        val rdd8 = sc.parallelize(List(1, 3, 5, 7, 8))
        val rdd9 = sc.parallelize(List(2, 4, 5, 6, 8, 9))
        val res1 = rdd8.union(rdd9)
        println("并集: " + res1.collect().toBuffer)
        // 5. 求交集
        val res2 = rdd8.intersection(rdd9)
        println("交集: " + res2.collect().toBuffer)
        // 6. 去重
        println(res1.distinct().collect().toBuffer)

        // 7. join, 该操作只能在PairRDD中使用（K, V），返回的是key-value类型的RDD中，
        // key匹配的所有元素，返回类型为：(K, (V1, V2)), 其中(K, V1)在当前RDD中，(K, V2)在另一个RDD中
        // 不匹配的值会被过滤掉, rdd的顺序只会影响(V1, V2)的顺序，不会影响整体结果
        val pairRdd1 = sc.parallelize(List(("Jack", 1), ("Kitty", 3), ("Kitty", 6), ("Love", 4)))
        val pairRdd2 = sc.parallelize(List(("Jack", 3), ("Kitty", 8), ("Funny", 9)))
        println("Join-Rdd1: " + pairRdd1.join(pairRdd2).collect().toBuffer)
        println("Join-Rdd2: " + pairRdd2.join(pairRdd1).collect().toBuffer)
        // 8. leftOutJoin, 左外连接，以左边的RDD作为基准，结果中包含左边RDD中的所有元素以及key匹配的右边的元素
        // 如果key在右边的RDD中没有匹配，返回(K, (V, Nome)), 否则返回(K, (V, Some(W)))
        // 下面的返回结果：(("Jack", (1, Some(3))), ("Kitty", (3, Some(8))), ("Love", (4, None)))
        println("Left-Join: " + pairRdd1.leftOuterJoin(pairRdd2).collect().toBuffer)
        // 9. rightOutJoin, 右外连接，以右边的RDD作为基准，左边不存在的key用None补齐
        println("Right-Join: " + pairRdd1.rightOuterJoin(pairRdd2).collect().toBuffer)
        // 10. 按key进行分组，只能在key-value的rdd中使用, 分组之后原来的key是分组之后的key，value是原来value组成的Iterator
        // (K, (Iterator[V]))
        println("分组：" + pairRdd1.union(pairRdd2).groupByKey().collect().toBuffer)
        // 11. 使用groupByKey和reduceByKey实现单词计数
        println("groupByKey统计次数：" + pairRdd1.union(pairRdd2).groupByKey().mapValues(_.sum).collect().toBuffer)
        println("reduceByKey统计：" + pairRdd1.union(pairRdd2).reduceByKey(_+_).collect().toBuffer)
        // 12. cogroup, 跟groupByKey类似，区别在于生成的结果(K, (Iterator[V1], Iterator[V2]))
        // 个人理解：groupByKey会将两个RDD中的key先结合然后按照key分组，
        // cogroup遍历每个RDD中的key，在内部分组，然后结合到一起
        // cogroup：   ((Love,(CompactBuffer(),CompactBuffer(4))), (Kitty,(CompactBuffer(8),CompactBuffer(3, 6)))
        // groupbykey：((Funny,CompactBuffer(9)), (Kitty,CompactBuffer(3, 6, 8))
        println("coGroup: " + pairRdd1.cogroup(pairRdd2).collect().toBuffer)
        println("coGroup: " + pairRdd2.cogroup(pairRdd1).collect().toBuffer)
        // 13. 按照value降序排序
        println("排序：" + pairRdd1.union(pairRdd2).reduceByKey(_+_).sortBy(-_._2).collect().toBuffer)
        // 14. 笛卡尔积
        println("笛卡尔：" + pairRdd1.cartesian(pairRdd2).collect().toBuffer)

    }
}
