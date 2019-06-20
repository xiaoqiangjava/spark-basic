package com.xq.spark.mllib.als

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * Feature Extractors
  * TF-IDF(Term frequency-inverse document frequency)
  */
object TFIDFDemo {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local")
            .appName(TFIDFDemo.getClass.getSimpleName)
            .getOrCreate()
        val sentenceDF = spark.createDataFrame(Seq(
            (0.0, "Hi I heard about Spark"),
            (0.0, "I wish Java could use case classes"),
            (1.0, "Logistic regression models are neat")
        )).toDF("label", "sentence")
        println("sentence DataFrame")
        sentenceDF.show(false)

        // A tokenizer that converts the input string to lowercase and then splits it by white space
        val tokenizer = new Tokenizer()
        tokenizer.setInputCol("sentence")
        tokenizer.setOutputCol("words")

        val wordsDF = tokenizer.transform(sentenceDF)
        println("word DataFrame")
        wordsDF.show(false)

        val hashingTF = new HashingTF()
        hashingTF.setInputCol("words")
        hashingTF.setOutputCol("rawFeatures")
        hashingTF.setNumFeatures(20)

        val featuresDF = hashingTF.transform(wordsDF)
        println("Feature DataFrame")
        featuresDF.show(false)

        val idf = new IDF()
        idf.setInputCol("rawFeatures")
        idf.setOutputCol("features")

        val idfModel = idf.fit(featuresDF)
        println(idfModel.explainParams())
        val rescaleDF = idfModel.transform(featuresDF)
        println("rescale DataFrame")
        rescaleDF.show(false)

        spark.close()
    }
}
