package com.xiaoqiang.spark.sql.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
 * spark sql中自定义函数
 * spark sql中列明可以直接使用字符串，或着使用$"columnName", 'columnName  来简写，会自动转成Column类型
 * 但是需要注意的是这种类型的转换需要隐式转换，因此要在使用之前导入spark.implicits._
 */
object UDFDemo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val spark = SparkSession.builder()
      .appName("udf-demo")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    // 读取文件
    val dataFrame = spark.read.json("spark-sql/src/main/resources/user.json")
    // 创建视图
    dataFrame.createOrReplaceTempView("user")
    // 注册一个udf函数，实现名称前面加一个固定前缀
    val prefix: UserDefinedFunction = spark.udf.register("prefix", (str: String) => "name: " + str)

    // 通过DSL语法使用udf函数时，需要接受udf注册之后的返回值
    dataFrame.select(prefix($"username"), $"age").show()
    dataFrame.select($"username", $"age" + 1, $"gender").show()

    // 在SQL语句中使用udf
    spark.sql("select prefix(username), age from user").show()

    spark.stop()
  }
}
