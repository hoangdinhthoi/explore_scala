package org.example.advancedAnalyticsWithSpark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._

// Decision Tree & Random Forest for classification

object chapter3 {
  private val conf = new SparkConf().setMaster("local[*]").setAppName("recommendationSystem")
  private val sc = new SparkContext(conf)
  private val rawData = sc.textFile("hdfs://user/covtype.data")
  private val data = rawData.map { line =>
    val values = line.split(',').map(_.toDouble)
    val featureVector = Vectors.dense(values.init)
    val label = values.last - 1
    LabeledPoint(label, featureVector)
  }
  private val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
  trainData.cache()
  cvData.cache()
  testData.cache()
  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
    val predictionsAndLabels = data.map(example =>
      (model.predict(example.features), example.label)
    )
    new MulticlassMetrics(predictionsAndLabels)
  }
  private val model = DecisionTree.trainClassifier(trainData, numClasses = 7, Map[Int,Int](), "gini", 4, 100)
  private val metrics = getMetrics(model, cvData)
}
