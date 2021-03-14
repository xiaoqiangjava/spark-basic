package com.xiaoqiang.spark.sql.udf

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Column, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.functions.col

/**
 * 自定义聚合函数：强类型聚合函数
 * 使用时必须使用DSL语法，不同直接在SQL中使用，在DSL语法中奖聚合函数转化成Column对象
 */
object UDAFTypeDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("udaf-type-demo")
      .getOrCreate()
    import spark.implicits._

    // 读取数据
    val userDF = spark.read.json("spark-sql/src/main/resources/user.json")
    // 当使用自定义的强类型聚合函数时，我们的DF需要转换成DS
    val userDS = userDF.as[User]

    val avgAggregator = new AvgAggregator()
    // 转换成列函数, 需要指定别名的话需要在这里直接指定，如果在使用时指定会报错
    val ageAvg: TypedColumn[User, Long] = avgAggregator.toColumn.name("ageAvg")
    // 使用聚合函数求年龄的平均值
    userDS.select($"username".as("name"), $"age").show()
    userDS.select(ageAvg).show()

    userDF.select(col("username"), new Column("age")).show()


    spark.stop()
  }
}

/**
 * 计算年龄的平均值, Aggregator是个强类型的聚合函数
 * 定义泛型：
 * IN：输入数据类型
 * BUF：缓存区数据类型
 * OUT：输出的数据类型
 */
class AvgAggregator extends Aggregator[User, Buffer, Long] {
  // 初始值或者0值，表示缓冲区的初始化
  override def zero: Buffer = Buffer(0, 0)

  // 根据输入的数据更新缓冲区的数据
  override def reduce(buff: Buffer, in: User): Buffer = {
    buff.total = buff.total + in.age
    buff.count = buff.count + 1

    buff
  }

  // 合并缓冲区
  override def merge(buff1: Buffer, buff2: Buffer): Buffer = {
    buff1.total = buff1.total + buff2.total
    buff1.count = buff1.count + buff2.count

    buff1
  }

  // 计算
  override def finish(reduction: Buffer): Long = reduction.total / reduction.count

  // 序列化，自定义的都是Encoders.product
  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  // 输出类型序列化：Scala中自动的类型使用Encoders.scalaInt
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

// buffer这里的值需要定义成var，因为默认情况是val，数据不能修改
case class Buffer(var total: Long, var count: Long)

case class User(username: String, age: Long)