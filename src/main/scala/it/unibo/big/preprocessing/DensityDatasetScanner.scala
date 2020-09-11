//package it.unibo.big.preprocessing
//
//import it.unibo.big.Utils
//import it.unibo.big.temporal.TemporalCellBuilder
//import it.unibo.big.temporal.TemporalCellBuilder.createAndAssignTemporalBuckets
//import org.apache.spark.sql.SaveMode
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.LongType
//import org.rogach.scallop.ScallopConf
//
//object DensityDatasetScanner {
//
//  def main(args: Array[String]): Unit = {
//
//    val sparkSession = createSparkSessionAndSetTrajectory()
//    val conf = new Conf(args)
//    val inputTable = conf.input_table_name()
//    val outputViewName = inputTable + "_temp_bucket"
//    val inputDF = TemporalCellBuilder.convertTimeInsideDataset(sparkSession, inputTable, conf.time_sampling_ratio().toInt)
//    createAndAssignTemporalBuckets(sparkSession, inputDF, outputViewName, 1)
//
//    val slicedTableDF = sparkSession.sql(s"select * from ${outputViewName}")
//      .withColumn("slice_timestamp", expr(s"${Utils.TIME_BUCKET_COLUMN_NAME}/${conf.time_range()}").cast(LongType))
//      .groupBy("slice_timestamp")
//      .count()
//      .withColumnRenamed("count", "n")
//
//    slicedTableDF.orderBy(desc("n"))
//      .show(100)
//
//   val topCountSliceID =
//    slicedTableDF
//    .orderBy(desc("n"))
//      .head()
//      .getAs[Long]("slice_timestamp")
//
//    println(s"Top slice is:$topCountSliceID")
//
//    sparkSession.sql(s"select * from ${outputViewName}")
//      .withColumn("slice_timestamp", expr(s"${Utils.TIME_BUCKET_COLUMN_NAME}/${conf.time_range()}").cast(LongType))
//      .filter(expr(s"slice_timestamp == ${topCountSliceID}"))
//      .drop("slice_timestamp")
//      .drop(s"${Utils.TIME_BUCKET_COLUMN_NAME}")
//      .drop(s"${Utils.BUCKET_TIMESTAMP_COLUMN_NAME}")
//      .write
//      .mode(SaveMode.Overwrite)
//      .saveAsTable(conf.output_table_name())
//  }
//
//
//  /**
//   * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
//   *
//   * @param arguments the programs arguments as an array of strings.
//   */
//  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//    val time_range = opt[Long](required = true)
//    val time_sampling_ratio = opt[Long](required = true)
//    val input_table_name = opt[String](required = true)
//    val output_table_name = opt[String](required = true)
//    verify()
//  }
//
//}
