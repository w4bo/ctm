//package it.unibo.big.preprocessing
//
//import java.text.SimpleDateFormat
//
//import it.unibo.big.Utils
//import it.unibo.big.temporal.weekly.WeeklyHourTimeStamp
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{Row, SparkSession}
//import org.rogach.scallop.ScallopConf
//
///**
// * This class will convert a table into a valid tsv file.
// */
//object TableDatasetConverter {
//
//  def main(args: Array[String]): Unit = {
//    val conf = new Conf(args)
//    val sparkSession = createSparkSessionAndSetTrajectory()
//    if(conf.time_sampling_ratio.isSupplied && conf.weeklyscale.isSupplied) throw new IllegalArgumentException("Ambiguous temporal scale, please specify only one")
//    if(conf.time_sampling_ratio.isEmpty && conf.weeklyscale.isEmpty) throw new IllegalArgumentException("No temporal scale specified")
//    var tableName = conf.input_table_name()
//    val tableTupleRDD = readTableRDD(sparkSession, tableName)
//    println("Terminated RDD conversion")
//    val stringTupleRDD = if(conf.time_sampling_ratio.isSupplied) computeDataset(tableTupleRDD, conf.time_sampling_ratio())
//    else computeDataset(tableTupleRDD, conf.weeklyscale())
//    println("Terminated String conversion")
//    checkOutputFolder(sparkSession.sparkContext, conf.output_tsv_path())
//    saveDatasetAsTSV(sparkSession.sparkContext, conf.output_tsv_path(), stringTupleRDD)
//  }
//
//  def processGeolifeTime(sparkSession: SparkSession, geolifeTableName:String):String = {
//    val conversionFunction:(String, String) => Long = (date, time) => {
//      val validDate = date + " " + time
//      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      dateFormat.parse(validDate).getTime / Utils.MILLIS_IN_SECOND
//    }
//    val udfFunction = udf(conversionFunction)
//    val tmp_geolife_table_name = s"tmp_${geolifeTableName}_absolute"
//    sparkSession.sql(s"select * from ${geolifeTableName}")
//        .withColumn("alt_timest", udfFunction(col("date"), col("timest")))
//        .drop("timest")
//        .withColumnRenamed("alt_timest", "timest")
//        .createOrReplaceTempView(tmp_geolife_table_name)
//    println("Terminated Geolife conversion")
//    tmp_geolife_table_name
//  }
//
//  def mapTuple(row: Row, timeStampFieldName: String): TABLE_TUPLE = {
//    (row.getAs[String](Utils.USER_ID_FIELD).toString,
//      row.getAs[String](Utils.TRAJECTORY_ID_FIELD),
//      row.getAs[Double](Utils.LATITUDE_FIELD_NAME),
//      row.getAs[Double](Utils.LONGITUDE_FIELD_NAME),
//      row.getAs[Long](timeStampFieldName)
//    )
//  }
//
//  private def readTableRDD(sparkSession:SparkSession, tableName:String):RDD[TABLE_TUPLE] = {
//    val timeStampFieldName = Utils.TIMESTAMP_FIELD_NAME
//    sparkSession.sql(s"select ${Utils.USER_ID_FIELD}, ${Utils.TRAJECTORY_ID_FIELD}, ${Utils.LATITUDE_FIELD_NAME}," +
//      s"${Utils.LONGITUDE_FIELD_NAME}, ${timeStampFieldName} from $tableName")
//      .rdd
//      .map(row => mapTuple(row, timeStampFieldName))
//  }
//  private def computeDataset(tableTupleRDD:RDD[TABLE_TUPLE], timeUnitSize:Long):RDD[String] =
//    computeUniqueIndexOnTable(tableTupleRDD)
//      .map(tuple =>s"${tuple._1}${TAB_SEPARATOR}${tuple._2}${TAB_SEPARATOR}${tuple._3}${TAB_SEPARATOR}${tuple._4 / timeUnitSize}")
//
//  private def computeDataset(tableTupleRDD:RDD[TABLE_TUPLE], weeklyScale:String):RDD[String] =
//    computeUniqueIndexOnTable(tableTupleRDD)
//      .map(tuple =>s"${tuple._1}${TAB_SEPARATOR}${tuple._2}${TAB_SEPARATOR}${tuple._3}${TAB_SEPARATOR}${
//        val weeklyDate = WeeklyHourTimeStamp(tuple._4.toLong)
//        if(weeklyScale.equals(it.unibo.big.TemporalScale.DailyScale.value)) {
//          weeklyDate.hourOfDay
//        } else {
//          weeklyDate.weeklyDay
//        }
//      }")
//
//  private def computeUniqueIndexOnTable(tableTupleRDD:RDD[TABLE_TUPLE]):RDD[TSV_TUPLE] = {
//      tableTupleRDD
//      .groupBy(row => row._1 + ":" + row._2)
//      .zipWithIndex()
//      .map(tuple => (tuple._1._2, tuple._2))
//      .flatMap(tuple => tuple._1.map( row =>
//        (tuple._2.toString, row._3, row._4, row._5)
//      ))
//  }
//  private def saveDatasetAsTSV(sparkContext: SparkContext, outputTSVPath:String, sourceDatasetRDD:RDD[String]):Unit = {
//    val headerRDD = sparkContext
//      .parallelize(Array(s"${Utils.TRAJECTORY_ID_FIELD}\t${Utils.LATITUDE_FIELD_NAME}\t${Utils.LONGITUDE_FIELD_NAME}\t${Utils.TIMESTAMP_FIELD_NAME}"))
//    headerRDD.union(sourceDatasetRDD)
//      .saveAsTextFile(outputTSVPath)
//  }
//
//  private def checkOutputFolder(sparkContext: SparkContext, hdfsOutputPath: String): Unit = {
//    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
//    val outputPath = new Path(hdfsOutputPath)
//    if (fileSystem.exists(outputPath)) fileSystem.delete(outputPath, true)
//  }
//
//  /**
//   * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
//   *
//   * @param arguments the programs arguments as an array of strings.
//   */
//  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//    val time_sampling_ratio = opt[Long]()
//    val weeklyscale = opt[String]()
//    val input_table_name = opt[String](required = true)
//    val output_tsv_path = opt[String](required = true)
//    verify()
//  }
//
//}
