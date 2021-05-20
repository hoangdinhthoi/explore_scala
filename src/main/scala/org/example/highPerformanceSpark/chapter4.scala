package org.example.highPerformanceSpark

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.broadcast

import scala.reflect.ClassTag


object chapter4 extends App {
  /**
   * RDD join
   * If the RDDs do not have known partitioners, they will need to be
   * shuffled so that both RDDs share a partitioner, and data
   * with the same keys lives in the same partitions. If they have
   * the same partitioner, the data may be co-located so as to avoid network transfer.
   *
   * As with most key/value operations, the cost of the join increases with the
   * number of keys and the distance the records have to travel in order to get to their
   * correct partition.
   */
  override def main(args: Array[String]): Unit = {
    val a = makeArray(1, 2, 3)
    println(a.mkString("\t"))
  }

  /**
   * Inner join (default)
   * When both RDDs have duplicate keys, the join can cause the size of the data to expand dramatically.
   * It may be better to perform a distinct or combineByKey operation to reduce the key space or to
   * use co-group to handle duplicate keys instead of producing the full cross product
   * If keys are not present in both RDDs you risk losing your data unexpectedly. It can be safer to use an outer join
   * Consider using filter/reduceByKey for reducing before the join to avoid a big shuffle of data
   */
  def innerJoin(score: RDD[(Long, Double)], addr: RDD[(Long, String)]): RDD[(Long, (Double, String))] = {
    val joinedRDD = score.join(addr)
    joinedRDD.reduceByKey((x, y) => if(x._1 > y._1) x else y)
  }

  def reduceBeforeJoin(score: RDD[(Long, Double)], addr: RDD[(Long, String)]): RDD[(Long, (Double, String))] = {
    score.reduceByKey((x, y) => if(x > y) x else y).join(addr)
  }

  /**
   * Spark also has leftOuterJoin / rightOuterJoin / FullOuterJoin
   */
  def fullOuterJoin(score: RDD[(Long, Double)], addr: RDD[(Long, String)]): RDD[(Long, (Double, String))] = {
    score.reduceByKey((x, y) => if (x > y) x else y).fullOuterJoin(addr).mapValues {
      case (Some(v1), Some(v2)) => (v1, v2)
      case (Some(v1), None) => (v1, "")
      case (None, Some(v2)) => (0.0, v2)
      case _ => (0.0, "")
    }
  }

  /**
   * The default implementation of a join in Spark is a shuffled hash join. The shuffled hash join ensures
   * that data on each partition will contain the same keys by partitioning the second dataset with the same
   * default partitioner as the first, so that the keys with the same hash value from both datasets are
   * in the same partition. While this approach always works, it can be more expensive than necessary
   * because it requires a shuffle. The shuffle can be avoided if:
   *  1 Both RDDs have a known partitioner.
   *  2 One of the datasets is small enough to fit in memory, in which case we can do a broadcast hash join
   */

  def joinWithPartitioner(score: RDD[(Long, Double)], addr: RDD[(Long, String)]): RDD[(Long, (Double, String))] = {
    val partitioner = addr.partitioner match {
      case Some(p) => p
      case _ => new HashPartitioner(addr.partitions.length)
    }
    score.reduceByKey(partitioner, (x, y) => if(x > y) x else y).join(addr)
  }

  /**
   * When one of the two RDDs is small -> use a broadcast hash join
   * A broadcast hash join pushes one of the RDDs (the smaller one) to each of the worker nodes
   * If one of your RDDs can fit in memory or can be made to fit in memory it is
   * always beneficial to do a broadcast hash join, since it doesn't require a shuffle
   */
  def manualBroadcastHashJoin[K : Ordering : ClassTag, V1 : ClassTag, V2 : ClassTag](bigRDD : RDD[(K, V1)], smallRDD : RDD[(K, V2)]): RDD[(K, (V1, V2))]= {
    val smallRDDLocal = smallRDD.collectAsMap()
    bigRDD.sparkContext.broadcast(smallRDDLocal)
    bigRDD.mapPartitions(iter => {
      iter.flatMap{
        case (k, v1 ) => smallRDDLocal.get(k) match {
            case None => Seq.empty[(K, (V1, V2))]
            case Some(v2) => Seq((k, (v1, v2)))
          }
      }
    }, preservesPartitioning = true)
  }

  /**
   * ClassTag usage example
   */
  def makeArray[T: ClassTag](e: T*): Array[T] = Array[T](e: _*)

  /**
   * Spark SQL Joins
   * The Spark optimizer is able to do more of the heavy lifting for you, although you also give up some of your control
   */

  /**
   * DataFrame joins
   * Spark's supported join types are inner, left_outer (aliased as outer), left_anti, right_outer, full_outer, and left_semi.
   * 1. The inner join is both the default and likely what you think of when you think of joining tables
   * 2. Left outer joins will produce a table with all of the keys from the left table, and any rows without matching keys
   * in the right table will have null values in the fields that would be populated by the right table. Right outer joins are the
   * same, but with the requirements reversed
   * 3. Full outer joins keep all records from both tables
   * 4. A left semi join is the same as filtering the left table for only rows with keys present in the right table.
   * 5. Left anti join also only returns data from the left table, but instead only returns records that are
   * not present in the right table
   */
  def joinDF(df1: DataFrame, df2: DataFrame, key: String, joinType: String): DataFrame = {
    df1.join(df2, df1(key) === df2(key), joinType)
  }

  /**
   * DataFrame broadcast hash joins
   * Spark also automatically uses the spark.sql.conf.autoBroadcastJoinThreshold to determine if a table should be broadcast.
   * Or we can manually instruct Spark to do broadcast hash joins.
   */
  def broadcastHashJoin(df1: DataFrame, df2: DataFrame, key: String, joinType: String): DataFrame = {
    df1.join(broadcast(df2), df1(key) === df2(key), joinType)
  }

  /**
   * Dataset Joins
   * Joining Datasets is done with joinWith, and this behaves similarly to a regular relational join, except the
   * result is a tuple of the different record types
   */
}
