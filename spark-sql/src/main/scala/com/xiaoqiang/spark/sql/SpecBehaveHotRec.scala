package com.xiaoqiang.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object SpecBehaveHotRec {
    def main(args: Array[String]): Unit = {
        if (args.length < 3) {
            System.exit(-1)
        }
        // 从参数中获取特定的用户行为
        val action = args(0)
        // 获取文件输入路径
        val inputPath = args(1)
        // 获取文件输出路径
        val outputPath = args(2)
        // 创建spark
        val spark = SparkSession.builder().appName("SpecHotRec").master("yarn").getOrCreate()
        // 读取文件内容，根据特定的用户行为，推荐商品
        val schema = StructType(StructField("userId", StringType, false)
            :: StructField("actionObj", StringType, false) :: StructField("objType", StringType, false)
            :: StructField("actionType", StringType, false) :: StructField("weight", DoubleType, true) :: Nil)
        val df = spark.read.schema(schema).format("csv").option("sep", ",").load(inputPath)
        df.createTempView("user_behave")
        val sql = "select objType, sum(weight) as weight from global_temp.user_behave where actionType = '" + action +
            "' group by objType, actionType order by weight desc"
        val dataDF = spark.sql(sql)
        dataDF.show()
        dataDF.write.save(outputPath)
        spark.close()
    }
}
