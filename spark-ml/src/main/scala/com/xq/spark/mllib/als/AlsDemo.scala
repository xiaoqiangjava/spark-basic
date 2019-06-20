package com.xq.spark.mllib.als

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * 基于模型的协同过滤：Alternating Least Squares(ALS)
  * ALS支持Explicit feedback and Implicit feedback, 即显示反馈和隐式反馈
  * ALS算法中的参数以及默认值
  * alpha: alpha for implicit preference (default: 1.0)
  * checkpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext (default: 10)
  * coldStartStrategy: strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop. (default: nan)
  * finalStorageLevel: StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)
  * implicitPrefs: whether to use implicit preference (default: false)
  * intermediateStorageLevel: StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)
  * itemCol: column name for item ids. Ids must be within the integer value range. (default: item, current: movieId)
  * maxIter: maximum number of iterations (>= 0) (default: 10, current: 10)
  * nonnegative: whether to use nonnegative constraint for least squares (default: false)
  * numItemBlocks: number of item blocks (default: 10)
  * numUserBlocks: number of user blocks (default: 10)
  * predictionCol: prediction column name (default: prediction)
  * rank: rank of the factorization (default: 10, current: 10)
  * ratingCol: column name for ratings (default: rating, current: rating)
  * regParam: regularization parameter (>= 0) (default: 0.1, current: 0.01)
  * seed: random seed (default: 1994790107)
  * userCol: column name for user ids. Ids must be within the integer value range. (default: user, current: userId)
  */
object AlsDemo {
    def main(args: Array[String]): Unit = {
        val input = args(0)
        val checkpoint = args(1)
        val rank = args(2)
        val regParam = args(3)
        val maxIter = args(4)
        // 创建sparkSession，读取评分文件
        val conf = new SparkConf()
        conf.setAppName("AlsDemo").setMaster("yarn")

        val spark = SparkSession.builder().config(conf).getOrCreate()
        // 设置检查点目录，防止StackOverflowError，在ALS算法中，只有设置了该目录，checkpointInterval参数才会生效
        spark.sparkContext.setCheckpointDir(checkpoint)
        // 定义schema
        val schema = StructType(StructField("userId", IntegerType, false) ::
            StructField("movieId", IntegerType, false) ::
            StructField("rating", DoubleType, false) ::
            StructField("timestamp", LongType, false) :: Nil)
        // 读取CSV文件，转成Rating
        val ratingDF = spark.read
                .schema(schema)
                .option("header", true)
                .option("sep", ",")
                .csv(input)
        ratingDF.show()
        ratingDF.printSchema()

        // 将数据分为训练集和测试集
        val Array(training, test) = ratingDF.randomSplit(Array(0.8, 0.2))
        training.show()
        test.show()

        // 在训练集上面使用ALS算法构建推理模型  Estimator
        val als = new ALS()
        als.setRank(rank.toInt)
        als.setMaxIter(maxIter.toInt)
        als.setRegParam(regParam.toDouble)
        als.setUserCol("userId")
        als.setItemCol("movieId")
        als.setRatingCol("rating")

        // explainParams方法可以得到所有参数的说明
        println("参数说明：")
        println(als.explainParams())
        // extractParamMap方法提取当前使用的参数列表
        println("提取参数：")
        println(als.extractParamMap())
        // 训练模型 transformer
        val model = als.fit(training)
        // 在测试集上面计算模型的均方根误差（RMSE）, 为了防止计算出来的是NAN，指定冷启动策略
        model.setColdStartStrategy("drop")
        val predictions = model.transform(test)

        val evaluator = new RegressionEvaluator()
        // 设置计算那种误差
        evaluator.setMetricName("rmse")
        // 设置标签
        evaluator.setLabelCol("rating")
        // 预测列名，在als算法中可以指定，默认值是prediction
        evaluator.setPredictionCol("prediction")
        val rmse = evaluator.evaluate(predictions)
        println(s"root-mean-square-error: $rmse")

        spark.close()
    }
}

/**
  * 电影评分数据
  * @param userId userId
  * @param movieId movieId
  * @param rating rating
  * @param timestamp timestamp
  */
case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Long)