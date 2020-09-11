package it.unibo.big.test

import it.unibo.big.Utils
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Define a trait to load a mock dataset as a temp table.
 */
trait TempTableLoader {

  /**
   * Load and store a dataset.
   */
  def loadAndStoreDataset(fileLines: Array[String], tempTableName: String, spark: SparkSession): Unit = loadAndStoreDataset(fileLines, tempTableName, spark, TempTableLoader.STD_MULTIPLICATIVE_FACTOR)

  /**
   * Load and store a dataset.
   */
  def loadAndStoreDataset(fileLines: Array[String], tempTableName: String, spark: SparkSession, multiplicativeFactor: Long): Unit
}

object TempTableLoader {

  val STD_MULTIPLICATIVE_FACTOR = Utils.SECONDS_IN_HOUR

  def apply(): TempTableLoader = new TempTableLoaderImpl()

  private class TempTableLoaderImpl() extends TempTableLoader {
    override def loadAndStoreDataset(fileLines: Array[String], tempTableName: String, spark: SparkSession, multiplicativeFactor: Long): Unit = {
      val inputRDD =
        spark
          .sparkContext
          .parallelize(fileLines)
          .map(_.split(Utils.FIELD_SEPARATOR))
          .map(e => Row(e(0), e(0), e(1).toDouble, e(2).toDouble, e(3).toLong * multiplicativeFactor))
      spark.createDataFrame(inputRDD, Utils.INPUT_REQUIRED_SCHEMA).createOrReplaceTempView(tempTableName)
    }
  }
}
