package org.example.advancedAnalyticsWithSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation._
import org.apache.spark.{SparkConf, SparkContext}

// Music recommendation system with collaborative filtering

object chapter2 {
  private val conf = new SparkConf().setMaster("local[*]").setAppName("recommendationSystem")
  private val sc = new SparkContext(conf)
  //  Each line of the file contains a user ID, an artist ID, and a play count, separated
  //  by spaces
  private val rawUserArtistData = sc.textFile("hdfs://userds/user_artist_data.txt")
  private val rawArtistData = sc.textFile("hdfs://userds/artist_data.txt")
  private val artistByID = rawArtistData.flatMap { line =>
    val (id, name) = line.span(_ != '\t')
    if (name.isEmpty) {
      None
    } else {
      try {
        Some((id.toInt, name.trim))
      } catch {
        case _: NumberFormatException => None
      }
    }
  }
  private val rawArtistAlias = sc.textFile("hdfs://userds/artist_alias.txt")
  private val artistAlias = rawArtistAlias.flatMap { line =>
    val tokens = line.split('\t')
    if (tokens(0).isEmpty) {
      None
    } else {
      Some((tokens(0).toInt, tokens(1).toInt))
    }
  }.collectAsMap()
  private val bArtistAlias = sc.broadcast(artistAlias)
  private val trainData = rawUserArtistData.map { line =>
    val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
    val finalArtistID =
      bArtistAlias.value.getOrElse(artistID, artistID)
    Rating(userID, finalArtistID, count)
  }.cache()
  private val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
  private val testUserID = 2093706
  private val recommendations = model.recommendProducts(testUserID, 5)

  def testFunc(rdd1: RDD[((Int, String), Double)], rdd2: RDD[((Int, String), String)]): RDD[((Int, String), (Double, String))] = {
    rdd1.join(rdd2)
  }

}
