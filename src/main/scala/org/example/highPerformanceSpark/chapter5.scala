package org.example.highPerformanceSpark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.reflect.ClassTag
import scala.util.Random

object chapter5 extends App {
  /**
   * Effective transformations
   */
  override def main(args: Array[String]): Unit = {

  }

  /**
   * Wide vs. narrow transformations
   * Wide transformations are those that require a shuffle, while narrow transformations are those that do not.
   * Narrow transformations, the child partitions (the partitions in the resulting RDD) depend on a known
   * subset of the parent partitions
   *
   * The 2012 paper that first presented the evaluation semantics for Spark, defines transformations with
   * narrow dependencies as those in which each partition of the parent RDD is used by at most one partition
   * of the child RDD. The creators define transformations with wide dependencies as transformations in which
   * multiple child partitions may depend on [each partition in the parent]
   *
   * In Spark terminology, we say that each series of narrow transformations can be computed in the same
   * stage of the query execution plan.
   * In contrast a shuffle associated with a wide dependency marks a new stage in the RDD evaluation.
   * Tasks must be computed on a single partition and the data needed to compute each partition of a wide
   * dependency may be spread across machines, transformations with wide dependencies may require data to be
   * moved across partitions. Thus, the downstream computations cannot be computed before the shuffle finishes.
   *
   * Thus, shuffles are not only expensive since they require data movement and potential disk
   * I/O (for the shuffle files), they also limit parallelization.
   */

  /**
   * Fault tolerance
   * The cost of failure for a partition with wide dependencies is much higher than for one with
   * narrow dependencies, since it requires more partitions to be recomputed
   */

  /**
   * Minimizing Object Creation
   * 1. Reuse existing object
   * 2. Use smaller data structure: An important way to optimize Spark jobs for both time and space is to
   * stick to primitive types rather than custom classes. Although it may make code less readable, using arrays
   * rather than case classes or tuples can reduce GC overhead.
   */

  /**
   * Iterator-to-Iterator Transformations with mapPartitions
   * The RDD mapPartitions function takes as its argument a function from an iterator of
   * records (representing the records on one partition) to another iterator of records (representing the output partition).
   */

  /**
   * Shared variables
   * 1. Broadcast variables
   * 2. Accumulators
   */

  class Lazy{
    @transient lazy val r = new Random()
  }
  def filterWithBroadcastVariables[T: ClassTag](sc: SparkContext, rdd: RDD[T]): RDD[T] = {
    val obj = sc.broadcast(new Lazy())
    rdd.filter(t => obj.value.r.nextInt(10) == 0)
  }

  /**
   * Accumulators
   */
  def accumulatorExample(sc: SparkContext, rdd: RDD[(Int, Long)]): Long = {
    val acc = sc.longAccumulator("cum sum")
    rdd.map(t => acc.add(t._2))
    acc.value
  }

  /**
   * Reuse RDD by persisting (in-memory) & checkpointing (write to external storage)
   * Iterative computations
   *
   * Even if a program does not use the same RDD multiple times, persisting and check-pointing can speed up
   * a routine and reduce the cost of failures by storing intermediary results. Persisting or checkpointing can
   * be particularly useful if the cost of computing one partition is very high because they ensure that the
   * entire expensive operation will not need to be recomputed in the case of downstream failures.
   */

  /**
   * There are three primary operations that you can use to store your RDD: cache, persist, and checkpoint
   * In general, caching (equivalent to persisting with the in-memory storage) and persisting are most useful
   * to avoid re-computation during one Spark job or to break RDDs with long lineages, since they keep an
   * RDD on the executors during a Spark job.
   * Checkpointing is most useful to prevent failures and a high cost of re-computation by saving intermediate results
   *
   * Spark provides a number of different storage levels as constants, but each one is created based on five
   * attributes of how to store the RDD: useDisk, useMemory, useOfHeap, deserialized, and replication
   * 1. useDisk: if set, If set, partitions that do not fit in memory will be written to disk.
   *    The storage-level flags containing DISK (such as MEMORY_AND_DISK) enable this. By default, if partitions
   *    do not fit in memory, they will simply be evicted and will need to be recomputed when the persisted RDD is used
   * 2. useMemory: If set, the RDD will be stored in-memory or be directly written to disk.
   *    The DISK_ONLY storage levels are the only options that mark this as false. Most of the speed benefits
   *    of caching come from keeping RDDs in memory, so if the motivation for reuse is fast access for
   *    repeated computations, it is probably a good idea to choose a storage option that stores partitions in memory
   * 3. useOfHeap: If set, the RDD will be stored outside of the Spark executor in an external system such as Tachyon
   *    The storage option off_heap enables this property. If memory is a serious issue, or a cluster is noisy
   *    and partitions are evicted, this option may be compelling.
   * 4. deserialized: If set, the RDD will be stored as deserialized Java objects.
   *    This can make storing RDDs more space efficient, especially when using a faster serializer—but incurs
   *    some performance overhead. Storage options that include the "_SER" suffix such as MEMORY_ONLY_SER
   *    enable serialization.
   * 5. replication: Replication is an integer that controls the number of copies of the persisted data
   *    to be stored in the cluster.
   *    By default this is set to 1; however, serialization options that end in _2 such as DISK_ONLY_2
   *    replicate each partition across two nodes
   *
   * Checkpointing
   * Checkpointing writes the RDD to an external storage system such as HDFS or S3, and (in contrast to persisting) forgets
   * the RDD’s lineage. Since checkpointing requires writing the RDD outside of Spark, checkpointed
   * information survives beyond the duration of a single Spark application and forces evaluation of an RDD
   */
  def persistAndCheckPoint[T](rdd: RDD[T], storageLevel: StorageLevel, checkpoint: Boolean, dir: String): Unit = {
    if(storageLevel != StorageLevel.NONE) rdd.persist(storageLevel)
    if(checkpoint) {
      rdd.sparkContext.setCheckpointDir(dir)
      rdd.checkpoint()
    }
  }

  /**
   * LRU caching
   * RDDs that are stored in memory and/or on disk in Spark are not automatically unpersisted when they are no
   * longer going to be used.
   * Spark uses Least Recently Used or LRU caching, to determine which partitions to evict if the executors
   * begin to run out of memory.
   * LRU caching behaves differently for different persistence options. For memory only persistence operations
   * configured with LRU caching, Spark will recompute the evicted partition each time it is needed. For memory
   * and disk options, LRU caching will write the evicted partition to disk. If you want to take a persisted RDD
   * out of memory to free up space, use unpersist.
   */

  /**
   * Caching & accumulator
   * If part of an RDD has to be recomputed, Spark may continue to add values to the accumulator as it recomputes;
   * causing the values in the recomputed part to be double counted.
   * Surprisingly, caching does not prevent either double counting or problems that arise from partially evaluated
   * partitions. Cached partitions may be evicted, so double counting may still arise if the machine with the cached
   * data fails or if the partition is evicted to make space for a more recently cached partition.
   */

}





































































