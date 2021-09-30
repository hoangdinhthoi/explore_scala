package org.example.advancedAnalyticsWithSpark

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

// Kmeans for anomaly detection

object chapter4 {
  private val conf = new SparkConf().setMaster("local[*]").setAppName("recommendationSystem")
  private val sc = new SparkContext(conf)
  private val rawData = sc.textFile("hdfs://userds/kddcup.data")
  // Count the label & print the result
  rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)
  private val labelsAndData = rawData.map { line =>
    val buffer = line.split(',').toBuffer
    buffer.remove(1, 3)
    val label = buffer.remove(buffer.length-1)
    val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
    (label,vector)
  }
  private val data = labelsAndData.values.cache()
  private val kmeans = new KMeans()
  private val model = kmeans.run(data)
  model.clusterCenters.foreach(println)

  // Distance between two data points
  def distance(a: Vector, b: Vector): Double =
    math.sqrt(a.toArray.zip(b.toArray).
      map(p => p._1 - p._2).map(d => d * d).sum)

  // Distance between a data point and its cluster
  def distToCentroid(datum: Vector, model: KMeansModel): Double = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }

  // Average distance of all data points and their cluster
  def clusteringScore(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  // Evaluate values of k, from 5 to 40
  (5 to 40 by 5).map(k => (k, clusteringScore(data, k))).foreach(println)


  // Normalize data point
  private val dataAsArray = data.map(_.toArray)
  private val numCols = dataAsArray.first().length
  private val n = dataAsArray.count()
  private val sums = dataAsArray.reduce(
    (a,b) => a.zip(b).map(t => t._1 + t._2))
  private val sumSquares = dataAsArray.fold(new Array[Double](numCols))(
    (a,b) => a.zip(b).map(t => t._1 + t._2 * t._2)
  )
  private val stdevs = sumSquares.zip(sums).map {
    case(sumSq,sum) => math.sqrt(n*sumSq - sum*sum)/n
  }
  private val means = sums.map(_ / n)

  def normalize(datum: Vector): Vector = {
    val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
      (value, mean, stdev) =>
        if (stdev <= 0) (value - mean) else (value - mean) / stdev
    )
    Vectors.dense(normalizedArray)
  }
  private val normalizedData = data.map(normalize).cache()
  // Evaluate values of k with data normalization
  (60 to 120 by 10).par.map(k =>
    (k, clusteringScore(normalizedData, k))).toList.foreach(println)

}
