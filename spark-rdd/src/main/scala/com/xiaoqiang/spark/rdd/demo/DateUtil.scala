package com.xiaoqiang.spark.rdd.demo

import java.text.SimpleDateFormat

object DateUtil {
    def calDutarion(date: String): Long = {
        val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        sdf.parse(date).getTime
    }
}
