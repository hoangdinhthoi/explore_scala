package org.example.highPerformanceSpark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object chapter2 {
  /**
   *
   * Narrow transformations are those in which each partition in the child
   * RDD has simple, finite dependencies on partitions in the parent RDD
   *
   * When Spark already knows the data is partitioned in
   * a certain way, operations with wide dependencies do not cause a shuffle
   */

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TEST")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Seq("a b c d", "a e f s e d f z e", "f e d j k k o k k k i d a a e p k k e d a"))
    simpleWOrdCount(rdd).foreach(println)
  }

  def simpleWOrdCount(rdd: RDD[String]): RDD[(String, Int)] = {
    val temp = rdd.flatMap(t => t.split(" ")).map(t => (t, 1))
    temp.reduceByKey(_ + _)
  }

  def wordCountFilteredStopWord(rdd: RDD[String], stopWords: Set[String]): RDD[(String, Int)] = {
    rdd.flatMap(t => t.split(" ")).filter(t => !stopWords.contains(t)).map(t => (t, 1)).reduceByKey(_ + _)
  }



}
