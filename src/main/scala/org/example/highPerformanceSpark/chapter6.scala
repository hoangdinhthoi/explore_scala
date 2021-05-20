package org.example.highPerformanceSpark

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object chapter6 extends App {
  /**
   * Working with Key/Value pair
   */
  case class Pandas(happiness: Double, niceness: Double, softness: Double, sweetness: Double)
  override def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val data = Seq(
      Pandas(15.0, 0.25, 2467.0, 0.0),
      Pandas(2.0, 1000, 25.4, 0.0),
      Pandas(10.0, 2.0, 50.0, 0.0),
      Pandas(3.0, 8.5, 0.2, 98.0)
    )
    val df = data.toDF
    val ranks_list = List(2, 4)
    val ranks_1 = findRankStatistics(df, ranks_list)
    println("Version 1")
    ranks_1.foreach(println)
    val ranks_2 = findRankStatisticsV2(df, ranks_list)
    println("Version 2")
    ranks_2.foreach(println)
    val ranks_3 = findRankStatisticsV3(df, ranks_list, 4)
    println("Version 3")
    ranks_3.foreach(println)
    val ranks_4 = findRankStatisticsV4(df, ranks_list)
    println("Version 4")
    ranks_4.foreach(println)
  }

  /**
   * Out of memory errors can easily happen in both the driver and executor nodes when working with key/value RDD
   * Two techniques to address these error (both associated with shuffles):
   * 1. Shuffle less often: One way to minimize the number of shuffles in a computation that requires several
   * transformations is to make sure to preserve partitioning across narrow transformations to avoid reshuffling data
   * 2. Shuffle better: Not all wide transformations and not all shuffles are equally expensive or prone to failure. By
   * using wide transformations such as reduceByKey and aggregateByKey that can preform map-side reductions and that
   * do not require loading all the records for one key into memory, you can prevent memory errors on the executors
   * and speed up wide transformations, particularly for aggregation operations
   */


  /** Raw data of the example
   * Panda name              Happiness  Niceness  Softness  Sweetness
   *  Mama Panda             15.0       0.25      2467.0    0.0
   *  Papa Panda             2.0        1000      35.4      0.0
   *  Baby Panda             10.0       2.0       50.0      0.0
   *  Baby Pandas toy Panda  3.0        8.5       0.2       98.0
   *
   * Design an application that would let her input an arbitrary list of integers n1…nk and return the nth
   * best element in each column
   */

  /**
   * Version 1
   * This solution works and is relatively robust, but it is very slow since it has to sort the data once for
   * each column and does so iteratively. In other words, if we have 8,000 columns we have to do 8,000 sorts!
   */
  def findRankStatistics(dataFrame: DataFrame, ranks: List[Int]) = {
    require(ranks.forall(_ > 0))
    val numberOfColumns = dataFrame.schema.length
    var i = 0
    var result = Map[Int, Iterable[Double]]()
    while (i < numberOfColumns) {
      val col = dataFrame.rdd.map(row => row.getDouble(i))
      val sortedCol = col.sortBy(v => v).zipWithIndex()
      val ranksOnly = sortedCol.filter {
        //rank statistics are indexed from one. e.g. first element is 0
        case (_, index) => ranks.contains(index + 1)
      }.keys
      val list = ranksOnly.collect()
      result += (i -> list)
      i += 1
    }
    result
  }

  /**
   * Since each sort can be done without knowledge of the other sorts, our intuition should be that it is possible
   * to parallelize this computation using each column as the unit of parallelization. We can represent the data as
   * one long list of key/value pairs where the keys represent the column indices. Then, we can perform our
   * computation in parallel for each key.
   * (key, value)
   * (1, 15.0)
   * (2, 0.25)
   * (3, 2467.0)
   * (4, 0.0)
   * (1, 2.0)
   * (2, 1000.0)
   * (3, 35.4)
   * (4, 0.0)
   * (1, 10.0)
   * (2, 2.0)
   * (3, 50.0)
   * (4, 0.0)
   * (1, 3.0)
   * (2, 8.5)
   * (3, 0.2)
   * (4, 98.0)
   */
  def mapToKeyValuePairs(dataFrame: DataFrame) = {
    val cols = dataFrame.schema.length
    dataFrame.rdd.flatMap(t => (0 until cols).map(v => (v, t.getDouble(v))))
  }

  /**
   * Version 2
   * GroupByKey solution
   * This solution has several advantages. First, it gives the correct answer. Second, it is very short and easy
   * to understand. It leverages out-of-the-box Spark and Scala functions and so it introduces few edge cases and
   * is relatively easy to test.
   * However, on data with a few million rows, we found that the solution failed consistently with memory
   * exceptions even on a many-node cluster.
   */
  def findRankStatisticsV2(dataFrame: DataFrame, ranks: List[Int]) = {
    def helper(arr: Array[Double], ranks: List[Int]): List[Double] = ranks match {
      case Nil => Nil
      case x::xs => arr(x) :: helper(arr, xs)
    }
    require(ranks.forall(_ > 0))
    val pairRDD = mapToKeyValuePairs(dataFrame)
    pairRDD.groupByKey().mapValues(
      t => helper(t.toArray.sortBy(v => v), ranks.map(t => t - 1))
    ).collectAsMap()
  }

  /**
   * Why GroupByKey fails
   * The groups created by groupByKey are always iterators, which can't be distributed. This causes an expensive
   * shuffled read step in which Spark has to read all of the shuffled data from disk and into memory.
   * In general it is better to choose aggregation operations that can do some map-side reduction to decrease
   * the number of records by key before shuffling (e.g., aggregateByKey or reduceByKey). If this is not possible,
   * using a wide transformation that does not require all the values associated with one key to be kept in-memory
   * is a good alternative to groupByKey. If you must use groupByKey, it is best if the next operation is an
   * iterator-to-iterator transformation
   */

  /**
   * Choosing an aggregation operation
   * 1. groupByKey:
   *    - Group values with the same key into a single iterator
   *    - Cannot have array keys with the default Hash Partitioner. To use array keys, use a custom partitioner.
   *    - If all of the records associated with any single key take up too much space in-memory to be read from disk on one executor
   *    - If there is not a known partitioner, this causes a shuffle. The shuffle gets more expensive as the number
   *    of distinct keys increases, the number of records per key increases, or the records are not evenly distributed across the keys.
   *    - Output partitioner: HashPartitioner
   * 2. combineByKey:
   *    - Combine values with the same key using a different result type
   *    - Cannot have array keys with the default Hash Partitioner. To use array keys, use a custom partitioner.
   *    - The "combine by" routine uses too much memory or creates too much garbage collection overhead, or the
   *    accumulator for one key becomes too large (this is the problem in groupByKey).
   * 3. aggregateByKey:
   *    - Same as combineByKey, but uses one zero value for all accumulators
   *    - To avoid memory allocation in aggregateByKey, modify the accumulator rather than return a new one.
   * 4. reduceByKey:
   *    - Combine values with the same key. Reduction must be to same type as original values.
   * 5. foldByKey:
   *    - Combine values with the same key using an associative combine function and a zero value, which can be
   *    added to the result an arbitrary number of times. Use instead of reduceByKey when a natural 0 exists
   */

  /**
   * Preventing out-of-memory error with aggregations operations
   * CombineByKey and all of the aggregation operators built on top of it (reduceByKey, foldLeft, foldRight, aggregateByKey)
   * are no better than groupByKey in terms of memory errors if they cause the accumulator to become too large for one key.
   * In fact, if you look up the implementation of groupByKey, you can see that it is actually implemented using combineByKey
   * where the accumulator is an iterator with all the data. Thus, the accumulator is the size of all the data for that key.
   * In other words, these operations are unlikely to cause memory errors as long as the combining steps make the data smaller.
   * However, if the accumulator gets larger with the addition of each new record, it will eventually cause memory
   * errors if there are many records associated with one key.
   *
   * Beyond being less likely to run out of memory than groupByKey, the following four functions (reduceByKey,
   * treeAggregate, aggregateByKey, and foldByKey) are implemented to use map-side combinations, meaning that records
   * with the same key are combined before they are shuffled. This can greatly reduce the shuffled read.
   */

  /**
   * Multiple RDD operations: Joins are far from the only multiple RDD operations. There are plenty more.
   * All of the join operations are implemented using the co-group function, which uses the CoGroupedRDD type. A
   * CoGroupedRDD is created from a sequence of key/value RDDs, each with the same key type.  cogroup shuffles each
   * of the RDDs so that the items with the same value from each of the RDDs will end up on the same partition and
   * into a single RDD by key.
   *
   * cogroup can be useful as an alternative to join when joining with multiple RDDs. Rather than doing joins on
   * multiple RDDs with one RDD it is more performant to co-partition the RDDs since that will prevent Spark
   * from shuffling the RDD being repeatedly joined.
   * Despite its advantages, cogroup will cause memory errors for the same reason as groupByKeys if one key in either
   * RDD or both combined is associated with more data than will fit on a single partition. In particular, cogroup
   * requires that all the records in all of the co-grouped RDDs for one key be able to fit on one partition
   */

  /**
   * Partitioners and Key/Value Data
   *
   * For RDDs of a generic record type, repartition and coalesce can be used to simply change the number of partitions
   * that the RDD uses, irrespective of the value of the records in the RDD. For RDDs of key/value pairs, we can use a
   * function called partitionBy, which takes a partition object rather than a number of partitions and shuffles the
   * RDD with the new partitioner. PartitionBy allows for much more control in the way that the records are partitioned
   * since the partitioner supports defining a function that assigns a partition to a record based on the value of that key.
   *
   * In all cases, repartition and coalesce do not assign a known partitioner to the RDD. In contrast, using partitionBy
   * (and most other key/value functions that cause a shuffle) results in an RDD with a known partitioner.
   */

  /**
   * Using the Spark Partitioner Object
   * Conceptually, the partitioner defines how records will be distributed and thus which records will be completed by
   * each task. Practically, a partitioner is actually an interface with two methods: numPartitions and getPartition
   * 1. numPartitions: defines the number of partitions in the RDD after partitioning.
   * 2. getPartition: defines a mapping from a key to the integer index of the partition where records with that key
   * should be sent.
   *
   *
   * Hash Partitioning & Range Partitioning
   * 1. HashPartitioning.
   *    A HashPartitioner determines the index of the child partition based on the hash value of the key. The hash
   *    partitioner requires a partitions parameter, which determines the number of partitions in the output RDD and
   *    the number of bins used in the hashing function.
   * 2. RangePartitioning.
   *    The range partitioner first determines the range bounds for each partition by sampling: optimizing for an equal
   *    distribution of records across partitions. Then, each record in the RDD will be shuffled to the partition whose
   *    range bounds include the key. Highly unbalanced data makes the sampling less accurate and may cause downstream
   *    tasks to be slower than others, causing straggler tasks. Sampling actually requires partially evaluating
   *    the RDD, causing a break in the execution graph. Thus range partitioning is actually both a transformation and
   *    an action. The cost of sampling means that, in general, range partitioning is more expensive than hash partitioning.
   */

  /**
   * Preserving Partitioning Information Across Transformations
   * Using narrow transformations that preserve partitioning
   */
  def demo(data: RDD[(Double, Int)]): Unit = {
    val sortedData = data.sortByKey()
    val mapValues: RDD[(Double, String)] = sortedData.mapValues(_.toString)
    assert(mapValues.partitioner.isDefined, "Using mapValues preserves partitioning")
    val map = sortedData.map( pair => (pair._1, pair._2.toString))
    assert(map.partitioner.isEmpty, "Using map does not preserve partitioning")
  }

  /**
   * Leveraging Co-Located and Co-Partitioned RDDs
   * Co-located RDDs are RDDs with the same partitioner that reside in the same physical location in memory.
   * Co-location is important because all of the CoGroupedRDD functions - a category which includes the cogroup
   * operations and all of the join operations — require the RDDs being grouped to have all of their partitions co-located.
   * RDDs can only be combined without any network transfer if they have the same partitioner and if each of the
   * corresponding partitions in-memory are on the same executor. Partitions will be in-memory on the same executor
   * if they were partitioned in the lineage associated with the same job.
   *
   * Multiple RDDs are co-partitioned if they are partitioned by the same known partitioner. Partitions are co-located
   * if they are both loaded into memory on the same machine. RDDs are only guaranteed to be co-located if they are put
   * into memory by the same job and the same partitioner.
   */

  /**
   * Mapping & partitioning functions on PairRDD
   * 1. mapValues
   *    - Slow when the mapping operation is very complicated or expands the size of each record
   *    - In contrast to map, this preserves the partitioning of the data for use in future operations. If the input
   *    RDD has a known partitioner, the output RDD will have the same partitioner. If you can perform your mapping on
   *    just the values it is almost always beneficial to do so
   * 2. flatMapValues
   *    - Slow when the mapping routine is very complicated, creates many new objects, or dramatically increases the
   *    number of records (such as tokenization).
   *    - Preserves partitioner associated with the input RDD. However, the distribution of duplicate values in the
   *    keys may change. If many duplicated values are created, this may slow down downstream shuffles.
   * 3. keys
   *    - Return an RDD of just the keys (not distinct)
   *    - Preserves the partitioning.
   * 4. values
   *    - Return an RDD of just the values
   *    - Does NOT preserve partitioning. Future wide transformations will cause a shuffle even if they have the same
   *    partitioner as the input RDD.
   * 5. sampleByKey
   *    - Given a map from the keys to the percent of each key to sample, returns a stratified sample of the input RDD.
   *    The function is implemented with mapPartitions and uses a random number to determine whether each record will
   *    be kept. Thus, the size of the resulting sample may not exactly correspond to the percentage specified.
   *    - Preserves partitioning of the input data.
   * 6. partitionBy
   *    - Takes a partitioner object and partitions the RDD accordingly. The partitioner object defines target partition
   *    index for records based on key.
   *    - Partitioned according to the partition argument
   */

  /**
   * Operations on OrderedRDD
   * 1. sortByKey
   *    - Return an RDD sorted by key
   *    - key must have an implicit Ordering
   *    - Output RDD will have RangePartitioner, the default number of partitions is the same as the input RDD
   * 2. repartitionAndSortWithinPartitions
   *    - Take a partitioner & an implicit ordering
   *    - Partition the RDD by the partitioner then sort all records on each partition according to the implicit ordering
   * 3. filterByRange
   *    - Take a lower & upper bound for the keys
   *    - Return an RDD of records whose key falls into the range
   *    - Preserve the partitioning scheme of the input RDD
   */

  /**
   * Use repartitionAndSortWithinPartitions for a Group-By-Key & sort values function
   * One common use case for this is to a define a function, which we might call “group by key and sort values
   * This approach could be generalized to any partitioning defined on any key type and any custom ordering. We use
   * repartitionAndSortWithinPartitions to repartition the RDD by one ordering on the keys and then define an implicit
   * ordering for the records on a given partition.
   * This groupByKeyAndSortValues function assumes data in the form ((k, s), v), where s is the secondary key (perhaps
   * derived from the value). It partitions the RDD by the first part of the key, then sorts by the second part of the
   * key. Then, it then combines all the values associated with one key into a sorted iterator.
   * The function has four steps:
   * 1. Define a custom partitioner that partitions records according to the first element of the key.
   * 2. Define an implicit ordering on the values. This is only necessary because the function is generic. The implicit
   *    ordering on tuples is first value, second value. We just have to tell Spark to use that tuple ordering.
   * 3. Use repartitionAndSortWithinPartitions on the input RDD with the custom partitioner defined in step 1
   * 4. Coalesce the items using a mapPartitions routine. We can leverage the fact that items with the same first key
   *    are on the same partition and that the elements within each partition are sorted first by the first ordering
   *    and then by the second ordering.
   */

  /**
   * Version 3: secondary sort
   * The logic of secondary sort generalizes well beyond simply ordering data. It applies to any use case that
   * requires the records to be arranged according to two different keys. The original Goldilocks example is related to
   * secondary sort since it requires us to shuffle on one key (the column index) and then order the data within each
   * key by value (by the value in the cells).
   * Thus, rather than using groupByKey to ensure that the values associated with each key are coalesced and then
   * sorting the elements associated with each key as a separate step, we can use repartitionAndSortWithinPartitions.
   * Using repartitionAndSortWithinPartitions we can partition on the column index and sort on the value in each column.
   * We are then guaranteed that all the values associated with each column will be on one partition and that they will
   * be in sorted order of the value.
   */

  /**
   * Define a custom Partitioner that partitions the keys based on the hash value of the first part of the
   * key (the column index)
   */
  class ColumnIndexPartition(override val numPartitions: Int) extends Partitioner {
    require(numPartitions >= 0, s"Number of partitions " + s"($numPartitions) cannot be negative.")
    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[(Int, Double)]
      Math.abs(k._1) % numPartitions
    }
  }

  def groupSorted(it: Array[(Int, Double)]): Map[Int, Iterable[Double]] = {
    val res = List[(Int, ArrayBuffer[Double])]()
    it.foldLeft(res)((list, next) => list match {
      case Nil =>
        val (firstKey, value) = next
        List((firstKey, ArrayBuffer(value)))
      case head :: rest =>
        val (curKey, valueBuf) = head
        val (firstKey, value) = next
        if (!firstKey.equals(curKey)) {
          (firstKey, ArrayBuffer(value)) :: list
        } else {
          valueBuf.append(value)
          list
        }
    }).toMap
  }

  /**
   * This solution is considerably faster than the version 1 groupByKey solution on any shape of data. By using
   * repartitionAndSortWithinPartitions, we are able to push the work to sort each column into the shuffle stage.
   * Since the elements are sorted after the shuffle, we are able to use iterator-to-iterator transformations to filter
   * the data and avoid forcing all the values associated with one partition into memory.
   */
  def findRankStatisticsV3(dataFrame: DataFrame, targetRanks: List[Int], partitions: Int) = {
    val pairRDD: RDD[((Int, Double), Int)] = mapToKeyValuePairs(dataFrame).map((_, 1))
    val partitioner = new ColumnIndexPartition(partitions)
    //sort by the existing implicit ordering on tuples first key, second key
    val sorted = pairRDD.repartitionAndSortWithinPartitions(partitioner)
    val filterForTargetIndex: RDD[(Int, Double)] = sorted.mapPartitions(iter => {
        var currentColumnIndex = -1
        var runningTotal = 0
        iter.filter({
          case (((colIndex, value), _)) =>
            if (colIndex != currentColumnIndex) {
              currentColumnIndex = colIndex //reset to the new column index
              runningTotal = 1
            } else {
              runningTotal += 1
            }
            //if the running total corresponds to one of the rank statistics.
            //keep this ((colIndex, value)) pair.
            targetRanks.contains(runningTotal)
        })
      }.map(_._1), preservesPartitioning = true)
    groupSorted(filterForTargetIndex.collect())
  }

  /**
   * Methods to make transformations more performant
   * 1. Narrow transformations on key/value data are quick and easy to parallelize relative to wide transformations
   * that cause a shuffle.
   * 2. Partition locality can be retained across some narrow transformations following a shuffle. This applies
   * to mapPartitions if we use preservePartitioning=true, or mapValues.
   * 3. Wide transformations are best with many unique keys. This prevents shuffles from directing a large proportion
   * of the data to reside on one executor.
   * 4. SortByKey is a particularly good way to partition data and sort within partitions since it pushes the ordering
   * of data on local machines into the shuffle stage.
   * 5. Using iterator-to-iterator transforms in mapPartitions prevents whole partitions from being loaded into memory.
   * 6. We can sometimes rely on shuffle files to prevent recomputation of wide transformations even if we call
   * several actions on the same RDD.
   */

  /**
   * Version 4
   * Alternative solution: The unit of parallelization is not necessarily the columns.
   * The solution can be enumerated in 5 steps:
   * 1. Map the rows of data to pairs of (cell value, index).
   * 2. Perform a sortByKey operation on all tuples defined in step 1.
   * 3. Using mapPartitions, determine how many elements in each column are on each partition and collect that
   *    information to the driver.
   * 4. Perform a local computation on the result of step 3 to determine the location of each desired rank statistic.
   *    For example, suppose that we are looking for the 13th element. Suppose also that in step 3 we determined that
   *    the first partition had 10 elements from column six. In this case, we can conclude that the 13th element will
   *    be the third largest element in column six on the second partition.
   * 5. Using the result of step 4, use another mapPartitions transformation to filter for the elements that correspond
   *    to the desired rank statistics. Collect this information back to the driver.
   */

  def mapToValueColumnPair(df: DataFrame) = {
    df.rdd.flatMap(t => t.toSeq.zipWithIndex.map{case (v, index) => (v.toString.toDouble, index)})
  }

  /**
   * Sort and count values on each partition
   */
  def getColumnsFreqPerPartition(sortedValueColumnPairs: RDD[(Double, Int)], numOfColumns : Int): Array[(Int, Array[Int])] = {
    val zero = Array.fill[Int](numOfColumns)(0)
    def aggregateColumnFrequencies (partitionIndex : Int, valueColumnPairs : Iterator[(Double, Int)]) = {
      val columnsFreq = valueColumnPairs.aggregate(zero)(
        (a : Array[Int], v : (Double, Int)) => {
          val (value, colIndex) = v
          //increment the cell in the zero array corresponding to this column index
          a(colIndex) = a(colIndex) + 1
          a
        },
        (a : Array[Int], b : Array[Int]) => {
          a.zip(b).map{ case(aVal, bVal) => aVal + bVal}
        })
      Iterator((partitionIndex, columnsFreq))
    }
    sortedValueColumnPairs.mapPartitionsWithIndex(aggregateColumnFrequencies).collect()
  }

  def getRanksLocationsWithinEachPart(targetRanks : List[Int], partitionColumnsFreq : Array[(Int, Array[Int])], numOfColumns : Int) : Array[(Int, List[(Int, Int)])] = {
    val runningTotal = Array.fill[Int](numOfColumns)(0)
    // The partition indices are not necessarily in sorted order, so we need
    // to sort the partitionsColumnsFreq array by the partition index (the
    // first value in the tuple).
    partitionColumnsFreq.sortBy(_._1).map { case (partitionIndex, columnsFreq) =>
      val relevantIndexList = new mutable.MutableList[(Int, Int)]()
      columnsFreq.zipWithIndex.foreach{ case (colCount, colIndex) =>
        val runningTotalCol = runningTotal(colIndex)
        val ranksHere: List[Int] = targetRanks.filter(rank =>
          runningTotalCol < rank && runningTotalCol + colCount >= rank)
        // For each of the rank statistics present add this column index and the
        // index it will be at on this partition (the rank - the running total).
        relevantIndexList ++= ranksHere.map(
          rank => (colIndex, rank - runningTotalCol))
        runningTotal(colIndex) += colCount
      }
      (partitionIndex, relevantIndexList.toList)
    }
  }

  def findTargetRanksIteratively(sortedValueColumnPairs : RDD[(Double, Int)], ranksLocations : Array[(Int, List[(Int, Int)])]): RDD[(Int, Double)] = {
    sortedValueColumnPairs.mapPartitionsWithIndex(
      (partitionIndex : Int, valueColumnPairs : Iterator[(Double, Int)]) => {
        val targetsInThisPart: List[(Int, Int)] = ranksLocations(partitionIndex)._2
        if (targetsInThisPart.nonEmpty) {
          val columnsRelativeIndex: Map[Int, List[Int]] =
            targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
          val columnsInThisPart = targetsInThisPart.map(_._1).distinct
          val runningTotals : mutable.HashMap[Int, Int]= new mutable.HashMap()
          runningTotals ++= columnsInThisPart.map(
            columnIndex => (columnIndex, 0)).toMap
          //filter this iterator, so that it contains only those (value, columnIndex)
          //that are the ranks statistics on this partition
          //Keep track of the number of elements we have seen for each columnIndex using the
          //running total hashMap.
          valueColumnPairs.filter{
            case(value, colIndex) =>
              lazy val thisPairIsTheRankStatistic: Boolean = {
                val total = runningTotals(colIndex) + 1
                runningTotals.update(colIndex, total)
                columnsRelativeIndex(colIndex).contains(total)
              }
              (runningTotals contains colIndex) && thisPairIsTheRankStatistic
          }.map(_.swap)
        }
        else {
          Iterator.empty
        }
      })
  }

  def findRankStatisticsV4(dataFrame: DataFrame, targetRanks: List[Int]) = {
    val valueColumnPairs: RDD[(Double, Int)] = mapToValueColumnPair(dataFrame)
    val sortedValueColumnPairs = valueColumnPairs.sortByKey()
    sortedValueColumnPairs.persist(StorageLevel.MEMORY_AND_DISK)
    val numOfColumns = dataFrame.schema.length
    val partitionColumnsFreq =
      getColumnsFreqPerPartition(sortedValueColumnPairs, numOfColumns)
    val ranksLocations = getRanksLocationsWithinEachPart(
      targetRanks, partitionColumnsFreq, numOfColumns)
    val targetRanksValues = findTargetRanksIteratively(
      sortedValueColumnPairs, ranksLocations)
    targetRanksValues.groupByKey().collectAsMap()
  }


  /**
   * Version 5: Reduce to Distinct on Each Partition
   */

}
