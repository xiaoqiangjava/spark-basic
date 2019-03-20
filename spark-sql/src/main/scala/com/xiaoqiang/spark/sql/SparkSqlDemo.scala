package com.xiaoqiang.spark.sql

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkSqlDemo {
    def main(args: Array[String]): Unit = {
        // 如果输入的参数小于两个, 结束程序
        if (args.length <2) {
            System.exit(-1);
        }
        // 获取文件输入路径
        val input = args(0)
        println("input: " + input)
        // 获取文件输出路径
        val output = args(1)
        println("ouput: " + output)
        // 创建sparkSession
        val spark = SparkSession.builder().appName("VideoCount")
            .master("yarn")
            .getOrCreate();
        val schema = StructType(Seq(StructField("uid", StringType, true), StructField("duration", IntegerType, true)))
        // 读取文件, 生成DataFrame-->DataSet[Row]-->Seq
        import spark.implicits._
        val df = spark.read.format("json").schema(schema).load(input)
            .select("uid", "duration")
            .na.fill(Map("uid" -> "0000", "duration" -> 0))
            .map(row => (row.getString(0), row.getInt(1)))
        // 将DataSet转化为RDD
        val videoRDD = df.rdd
        // 将seq装换为RDD, 进行相应的Transformation
        // 统计用户直播时长
        val durationRDD = videoRDD.reduceByKey(_+_).sortBy(-_._2)
        // 将结果保存到输出路径
        durationRDD.saveAsTextFile(output)
        spark.close()
    }
}
