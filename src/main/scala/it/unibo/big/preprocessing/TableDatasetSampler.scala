//package it.unibo.big.preprocessing
//
//import it.unibo.big.Utils
//import it.unibo.big.temporal.TemporalCellBuilder.{convertTimeInsideDataset, createAndAssignTemporalBuckets}
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.log4j.{Level, LogManager, Logger}
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{Row, SparkSession}
//import org.rogach.scallop.ScallopConf
//
///**
// * This class will select the top n trajectories from a dataset and it will save a table and a valid TSV dataset file.
// * Both the table and TSV will contains the exact same data with the exact same trajectories id.
// */
//object TableDatasetSampler {
//
//  def main(args: Array[String]): Unit = {
//
//    val conf = new Conf(args)
//    val spark = createSparkSessionAndSetTrajectory()
//
//    val sc = spark.sparkContext
//    sc.setLogLevel("ERROR")
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    Logger.getLogger("akka").setLevel(Level.ERROR)
//    LogManager.getRootLogger.setLevel(Level.ERROR)
//
//    val cuteTableOutputName: String = s"tmp_${conf.output_table_name()}"
//    val filteredcuteTableName: String = s"tmp_filtered_${conf.input_table_name()}"
//    val limit = conf.limit.getOrElse(Long.MaxValue)
//
//    spark.sql("use trajectory")
//    spark.sql(s"drop table if exists $cuteTableOutputName")
//    checkOutputFolder(spark.sparkContext, conf.output_tsv_path())
//
//    var gcmpInputTable = filteredcuteTableName
//
//    if (limit == Long.MaxValue) {
//      gcmpInputTable = conf.input_table_name()
//    } else {
//      spark.sql(s"select * from ${conf.input_table_name()} where ${Utils.TRAJECTORY_ID_FIELD} <= $limit").createOrReplaceTempView(filteredcuteTableName)
//    }
//
//    println("Starting TSV processing")
//    val inputDF = convertTimeInsideDataset(spark, gcmpInputTable, conf.time_sampling_ratio().toInt)
//    val outputViewName = gcmpInputTable + "_temp_time_bucket"
//    createAndAssignTemporalBuckets(spark, inputDF, outputViewName, 1)
//
//    println("Distinct time instants are: " + spark.sql(s"select distinct(${Utils.TIME_BUCKET_COLUMN_NAME}) from ${outputViewName}").count())
//    println("After transformation there are time instants: " + computeUniqueIndexOnTable(spark,outputViewName).select(Utils.TIMESTAMP_FIELD).distinct().count())
//
//    val transformedRDD = computeUniqueIndexOnTable(spark,outputViewName)
//      .rdd
//      .map(row => ((row.getAs[String](Utils.TRAJECTORY_ID_FIELD), row.getAs[Long](Utils.TIMESTAMP_FIELD)), row))
//      .groupByKey()
//      .map(_._2.head)
//      .map(row => (row.getAs[String](Utils.TRAJECTORY_ID_FIELD), row.getAs[Double](Utils.LATITUDE_FIELD_NAME),
//        row.getAs[Double](Utils.LONGITUDE_FIELD_NAME), row.getAs[Long](Utils.TIMESTAMP_FIELD)))
//    println("After RDD i will store timestamps: " + transformedRDD.map(_._4).distinct().count())
//    storeSampledDataset(spark, conf.input_table_name(), conf.time_sampling_ratio(), transformedRDD, cuteTableOutputName)
//    val headerRDD = sc.parallelize(Array(s"${Utils.TRAJECTORY_ID_FIELD}\t${Utils.LATITUDE_FIELD_NAME}\t${Utils.LONGITUDE_FIELD_NAME}\t${Utils.TIMESTAMP_FIELD}"))
//    headerRDD.union(transformedRDD.map(row => s"${row._1}\t${row._2}\t${row._3}\t${row._4}"))
//      .saveAsTextFile(conf.output_tsv_path())
//    println("Terminated TSV processing")
//  }
//
//  private def computeUniqueIndexOnTable(sparkSession: SparkSession, tableName: String) = {
//    val formattedInputDF = sparkSession.sql(s"select ${Utils.USER_ID_FIELD}, ${Utils.TRAJECTORY_ID_FIELD}," +
//      s"${Utils.LATITUDE_FIELD_NAME}, ${Utils.LONGITUDE_FIELD_NAME}, ${Utils.TIME_BUCKET_COLUMN_NAME} from $tableName")
//    val uniqueIndexRDD = formattedInputDF
//      .rdd
//      .map(row => TableDatasetConverter.mapTuple(row, Utils.TIME_BUCKET_COLUMN_NAME))
//      .groupBy(row => row._1 + ":" + row._2)
//      .zipWithIndex()
//      .map(tuple => (tuple._1._2, tuple._2))
//      .flatMap(tuple => tuple._1.map(row =>
//        Row(tuple._2.toString, tuple._2.toString, row._3,
//          row._4, row._5)
//      ))
//    sparkSession.createDataFrame(uniqueIndexRDD, Utils.INPUT_REQUIRED_SCHEMA)
//  }
//  private def checkOutputFolder(sparkContext: SparkContext, hdfsOutputPath: String): Unit = {
//    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
//    val outputPath = new Path(hdfsOutputPath)
//    if (fileSystem.exists(outputPath)) fileSystem.delete(outputPath, true)
//  }
//
//  private def storeSampledDataset(sparkSession: SparkSession, datasetTableName: String, timeBucketUnit: Long,
//                                  gcmpRDD: RDD[(String, Double, Double, Long)], outputTableName: String): Unit = {
//    println(s"Storing sampled trajectories to output table: $outputTableName")
//    val outputRDD = gcmpRDD.map(row => {
//      Row(row._1, row._1, row._2, row._3, row._4 * timeBucketUnit)
//    })
//    sparkSession.createDataFrame(outputRDD, Utils.INPUT_REQUIRED_SCHEMA)
//      .write
//      .mode("overwrite")
//      .saveAsTable(outputTableName)
//    println(s"Table $datasetTableName stored")
//  }
//
//  /**
//   * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
//   *
//   * @param arguments the programs arguments as an array of strings.
//   */
//  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//    val time_sampling_ratio = opt[Long](required = true)
//    val input_table_name = opt[String](required = true)
//    val output_table_name = opt[String](required = true)
//    val output_tsv_path = opt[String](required = true)
//    val limit = opt[Long]()
//    verify()
//  }
//}
