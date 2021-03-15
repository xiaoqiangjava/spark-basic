package com.xiaoqiang.spark.sql.functions

import java.util.Properties

import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * join操作
 * === 方法表示判断是否相等，对null不安全
 * <=> 对null值安全的判断相等的方法
 */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val spark = SparkSession.builder()
      .appName("join-demo")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    // 通过jdbc读取数据，这里配置每次读取的数据量为2条
    val prop = new Properties()
    val url = "jdbc:mysql://localhost:3306/join_test"
    prop.setProperty("fetchsize", "2")
    prop.setProperty("user", "root")
    prop.setProperty("password", "xxx")

    val empDF = spark.read.jdbc(url, "emp", prop)
    val deptDF = spark.read.jdbc(url, "dept", prop)
    val joinExprs: Column = $"emp.DEPTNO" === $"dept.DEPTNO" && ($"emp.ENAME" <=> "SMITH" or $"emp.ENAME" <=> "ALLEN")
    val joinDF = empDF.as("emp").join(deptDF as "dept", joinExprs)
      .select($"emp.ENAME".as("ENAME"), $"emp.EMPNO".as("EMPNO"), $"emp.DEPTNO".as("DEPTNO"))

    // 保存到hive
    joinDF.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("emp")

    spark.stop()
  }
}
