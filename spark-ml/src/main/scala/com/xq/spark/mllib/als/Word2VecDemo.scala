package com.xq.spark.mllib.als

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

object Word2VecDemo {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local")
            .appName(TFIDFDemo.getClass.getSimpleName)
            .getOrCreate()
        val documentDF = spark.createDataFrame(Seq(
            "Hi I heard about Spark".split(" "),
            "I wish Java could use case classes".split(" "),
            "Logistic regression models are neat".split(" ")
        ).map(Tuple1.apply)).toDF("text")
        documentDF.show(false)

        val word2Vec = new Word2Vec()
        word2Vec.setInputCol("text")
        word2Vec.setOutputCol("result")
        word2Vec.setVectorSize(300)
        word2Vec.setMinCount(0)

        // 训练模型
        val word2VecModel = word2Vec.fit(documentDF)
        val result = word2VecModel.transform(documentDF)
        result.show(false)

        // 查找与给定单词的余弦相似度最接近的词
        val sim = word2VecModel.findSynonyms("Java", 1)
        sim.show(false)

        spark.close()
    }
}
