package com.xiaoqiang.spark.sql.udf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

/**
 * 自定义聚合函数, 这里使用弱类型实现
 */
object UDAFDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("udaf-demo")
      .getOrCreate()
    import spark.implicits._

    // 读取数据
    val userDF = spark.read.json("spark-sql/src/main/resources/user.json")

    val ageAvg = spark.udf.register("ageAvg", new AvgFunction)

    // 使用聚合函数求年龄的平均值
    userDF.select(ageAvg($"age")).show()

    spark.stop()
  }
}

/**
 * 计算年龄的平均值
 * UserDefinedAggregateFunction是个弱类型的用户自定义聚合函数，操作时需要通过Row的下标来获取数据
 */
class AvgFunction extends UserDefinedAggregateFunction {
  // 输入的数据类型：计算年龄，传入年龄，是个Int类型的值
  override def inputSchema: StructType = StructType(StructField("age", IntegerType) :: Nil)

  // 缓冲区的数据结构，缓冲区应该计算总值和累加次数
  override def bufferSchema: StructType = StructType(StructField("total", IntegerType)
    :: StructField("count", IntegerType) :: Nil)

  // 函数计算结果的数据类型
  override def dataType: DataType = IntegerType

  // 函数的稳定性：传入相同的数值，结果是否相同
  override def deterministic: Boolean = true

  // 缓存区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // buffer是个Row类型，初始化数据时需要使用位置来初始化
    // 0表示缓存区bufferSchema中定义的数据结构中的一个只，也就是total
    buffer(0) = 0 // 这种写法跟buffer.update(0, 0)一样
    buffer(1) = 0
  }

  // 根据输入的值更新缓冲区中的值
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // input的数据结构是inputSchema函数中定义的数据类型
    buffer(0) = buffer.getInt(0) + input.getInt(0)
    buffer(1) = buffer.getInt(1) + 1
  }

  // 缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 更新缓冲区中的值，相同位置的值进行合并
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  // 计算，计算平均值
  override def evaluate(buffer: Row): Any = buffer.getInt(0) / buffer.getInt(1)
}