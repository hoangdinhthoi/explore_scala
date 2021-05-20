package org.example.highPerformanceSpark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object chapter3 extends App {
  case class Person(name: String, age: Int, address: String)

  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test"))
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val data = Seq(
      Person("a", 10, "123"),
      Person("b", 14, "sdf"),
      Person("c", 40, "1r"),
      Person("d", 20, "1z")
    )
    val rdd = sc.parallelize(data)
    val df = createDataFrameFromRDD(spark, rdd)
    saveDF(df, "C:\\Users\\HDT\\Desktop\\data\\data.json")
    println("OK")
  }

  /**
   * DataFrameWriter and DataFrameReader
   */
  def loadJson(spark: SparkSession, path: String): DataFrame = {
    spark.read.format("json").option("samplingRatio", "1.0").load(path)
  }

  /**
   * JDBC source
   */
  def loadJDBC(spark: SparkSession, jdbcServer: String, table: String): DataFrame = {
    spark.read.format("jdbc").option("url", jdbcServer).option("dbtable", table).load()
  }

  /**
   * Parquet
   */
  def loadParquet(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("mergeSchema", "true").format("parquet").load(path)
  }

  /**
   * DataFrame from RDD
   */
  def createDataFrameFromRDD(spark: SparkSession, rdd: RDD[Person]): DataFrame = {
//    import spark.implicits._
//    val df1 = spark.createDataFrame(rdd)
//    val df2 = rdd.toDF
    val rddRow = rdd.map(t => Row(t.name, t.age, t.address))
    val dataType = StructType(List(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("address", StringType, nullable = true)
    ))
    val df3 = spark.createDataFrame(rddRow, dataType)
    df3
  }

  /**
   * Save modes
   * ErrorIfExists / Append / Overwrite / Ignore
   */
  def saveDF(df: DataFrame, path: String): Unit = {
    df.write.mode(SaveMode.Overwrite).json(path)
  }

  /**
   * Save data by partitions (only string & numeric data are supported)
   */
  def saveDFByPartitions(df: DataFrame, path: String, column: String): Unit = {
    df.repartition(col(column)).write.partitionBy(column).json(path)
  }

  /**
   * Convert DataFrame to Dataset
   */
  def convert(spark: SparkSession, df: DataFrame): Dataset[Person] = {
    import spark.implicits._
    df.as[Person]
  }

}
