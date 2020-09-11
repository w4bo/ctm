package it.unibo.big

import org.apache.spark.sql.SparkSession

package object preprocessing {

  private def startSparkSession():SparkSession =
     SparkSession.builder()
    .enableHiveSupport()
    .config("spark.executor.instances", 9)
    .config("spark.executor.cores", 3)
    .config("spark.executor.memory", "12g")
    .getOrCreate()

  /**
   * Create a new spark session and set the default database to trajectory.
   * @return the created spark session
   */
  def createSparkSessionAndSetTrajectory():SparkSession = {
    val sparkSession = startSparkSession()
    sparkSession.sql("use trajectory")
    sparkSession
  }

  /**
   * Define the field found inside a tuple selected from a table during RDD computation.
   */
  type TABLE_TUPLE = (String, String, Double, Double, Long)

  /**
   * Define field inside a tuple inside a tsv RDD during computation.
   */
  type  TSV_TUPLE = (String, Double, Double, Long)

  val TAB_SEPARATOR = "\t"


}
