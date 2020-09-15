package it.unibo.big

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}
import java.util.function.Consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.roaringbitmap.RoaringBitmap

import scala.math._

/**
 * Utility object.
 */
object Utils {
  /** DB name */
  var DB_NAME = "CTM"
  /** Define the default cell size in meters. */
  val DEFAULT_CELL_SIDE: Int = 123
  /** Default storage threshold */
  val STORAGE_THR = 0
  /** Default executors */
  val NEXECUTORS = 1
  /** Defaultcores */
  val NCORES = 3
  /** Default RAM */
  val MAXRAM = "12g"
  /** Default shuffle partitions */
  val SPARK_SQL_SHUFFLE_PARTITIONS = 200
  /** Default shuffle partitions */
  val SPARK_SQL_TEST_SHUFFLE_PARTITIONS = NEXECUTORS * NCORES
  /**  TSV separator. */
  val FIELD_SEPARATOR = "\\t"
  /**  Column name for the timestamp inside the trajectory and cell tables. */
  val BUCKET_TIMESTAMP_COLUMN_NAME: String = "bucket_unix_timestamp"
  /** Column name for the time bucket inside the trajectory and cell tables. */
  val TIME_BUCKET_COLUMN_NAME: String = "time_bucket"
  /** Define how many milliseconds are inside a second. */
  val MILLIS_IN_SECOND: Int = 1000
  /** Define how many second are inside an hour. */
  val SECONDS_IN_HOUR: Int = 3600
  /** Define how many seconds are inside and hour. */
  val SECOND_IN_MINUTES: Int = 60
  /** Time distance column name for the space table. */
  val SPACE_DISTANCE_COLUMN_NAME = "space_distance"
  /** Time distance column name for the neighbourhood table. */
  val TIME_DISTANCE_COLUMN_NAME = "time_distance"
  /** Latitude field name. */
  val LATITUDE_FIELD_NAME = "latitude"
  /** Longitude field name. */
  val LONGITUDE_FIELD_NAME = "longitude"
  /**  Trajectory id field name. */
  val TRAJECTORY_ID_FIELD = "trajectoryid"
  /**  Custom id field name. */
  val USER_ID_FIELD = "userid"
  /**  Timestamp field name. */
  val TIMESTAMP_FIELD = "timestamp"
  /**  Default schema for every input database that will be processed by CTM algorithm. */
  val INPUT_REQUIRED_SCHEMA = StructType(
    Array(
      StructField(USER_ID_FIELD, StringType),
      StructField(TRAJECTORY_ID_FIELD, StringType),
      StructField(LATITUDE_FIELD_NAME, DoubleType),
      StructField(LONGITUDE_FIELD_NAME, DoubleType),
      StructField(TIMESTAMP_FIELD, LongType)
    )
  )

  /** Create a file at the specified path if it does not exists and write the stats tsv header. */
  def writeStatsToFile(fileName: String, inTable: String, minsize: Int, minsup: Int, nItemsets: Long,
                       storage_thr: Int, repfreq: Int, limit: Int, // EFFICIENCY PARAMETERS
                       nexecutors: Int, ncores: Int, maxram: String, // SPARK CONFIGURATION
                       timescale: TemporalScale, unit_t: Int, bin_t: Int, eps_t: Double,
                       bin_s: Int, eps_s: Double, // EFFECTIVENESS PARAMETERS
                       nTransactions: Long, brdTrajInCell_bytes: Int): Unit = {
    val fileExists = Files.exists(Paths.get(fileName))
    val outputFile = new File(fileName)
    outputFile.createNewFile()
    val bw = new BufferedWriter(new FileWriter(fileName, fileExists))
    if (!fileExists) {
      bw.write("time(ms),brdTrajInCell_bytes,brdCellInTraj_bytes,nTransactions,nItems,inTable,minsize,minsup,nItemsets,storage_thr,repfreq,limit,nexecutors,ncores,maxram,timescale,unit_t,bin_t,eps_t,bin_s,eps_s\n".replace("_", "").toLowerCase)
    }
    bw.write(s"${CustomTimer.getElapsedTime()},$brdTrajInCell_bytes,$nTransactions,$inTable,$minsize,$minsup,$nItemsets,$storage_thr,$repfreq,$limit,$nexecutors,$ncores,$maxram,$timescale,$unit_t,$bin_t,$eps_t,$bin_s,$eps_s\n")
    bw.close()
  }

  /**
   * Define a file where at the end of the computation is stored the total time, to be used for comparison.
   * For each configuration of all parameters, only one time is stored, if another run is executed the previous time will be deleted
   * so be careful.
   * @param timeToWrite a long containing the time to be written.
   */
  def writeTimeOnFile(timeToWrite: Long, fileTimeName: String): Unit = {
    val timeFileExists = Files.exists(Paths.get(fileTimeName))
    val timeOutputFile = new File(fileTimeName)
    if (timeFileExists) {
      timeOutputFile.delete()
    }
    timeOutputFile.createNewFile()
    println(s"Writing $timeToWrite on $fileTimeName")
    val bw = new BufferedWriter(new FileWriter(fileTimeName))
    bw.write(s"${timeToWrite}")
    bw.close()
  }

  /** Alias to clarify if the data is a transaction id (i.e., a cell id). */
  type Tid = Int
  /** Clarify if the data is an item (i.e., a trajectory id). */
  type Itemid = Int
  /**  Define a carpenter row set data type. */
  type CarpenterRowSet = (RoaringBitmap, Boolean, RoaringBitmap, RoaringBitmap)

  object MyMath {
    def roundAt(n: Double, pos: Int): Double = {
      // new BigDecimal(n).setScale(pos, BigDecimal.ROUND_HALF_UP).doubleValue()
      val s = math pow(10, pos)
      (math floor n * s) / s
    }
  }

  /** Distance estimation. */
  object Distances {
    /** Approximation of degree length in km. */
    val deglen = 110.25 // km
    /** Earth radius in km. */
    val R = 6372.8 //radius in km

    /**
     * Euclidean approximation of haversine distance in kilometers.
     * @param lat1 point1 latitude
     * @param lon1 point1 longitude
     * @param lat2 point2 latitude
     * @param lon2 point2 longitude
     * @return distance in km
     */
    def haversineEuclideanApproximation(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
      val x = lat1 - lat2
      val y = (lon1 - lon2) * Math.cos(lat2)
      deglen * Math.sqrt(x * x + y * y)
    }

    /**
     * Euclidean distance in meters.
     * @param lat1 point1 latitude
     * @param lon1 point1 longitude
     * @param lat2 point2 latitude
     * @param lon2 point2 longitude
     * @return distance in m
     */
    def euclideanDistance(lat1: Double, lat2: Double, lon1: Double, lon2: Double): Double = {
      val x = lat1 - lat2
      val y = lon1 - lon2
      Math.sqrt(x * x + y * y)
    }

    /**
     * Haversine distance in kilometers.
     * @param lat1 point1 latitude
     * @param lon1 point1 longitude
     * @param lat2 point2 latitude
     * @param lon2 point2 longitude
     * @return distance in km
     */
    def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
      val dLat = (lat2 - lat1).toRadians
      val dLon = (lon2 - lon1).toRadians
      val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
      val c = 2 * asin(sqrt(a))
      R * c
    }
  }

  /** GeoJSON conversion. */
  object GeoJSON {
    def toPointGeoJSON(lon: Double, lat: Double, roundat: Int = 8): String = "{\"type\":\"Point\",\"coordinates\":[" + MyMath.roundAt(lon, roundat) + "," + MyMath.roundAt(lat, roundat) + "]}"
  }

  /** Keep track of elapsed time. */
  object CustomTimer {
    var startTime = 0L
    var time = 0L
    /** Start the timer. */
    def start(): Unit = {
      startTime = System.currentTimeMillis()
      time = startTime
    }
    /** @return elapsed time */
    def getElapsedTime(): Long = System.currentTimeMillis() - startTime
    /** @return elapsed time since previous invocation of this method  */
    def getRelativeElapsedTime(): Long = {
      val newtime = System.currentTimeMillis() - time
      time = newtime
      time
    }
  }

  /**
   * Scala function to Java consumer
   * @param consumer Scala function
   * @tparam T parameter type
   * @return a consumer wrapping the function
   */
  def toJavaConsumer[T](consumer: (T) => Unit): Consumer[T] = new Consumer[T] { override def accept(t: T): Unit = consumer(t) }

  /**
   * @param appName    appname
   * @param nexecutors executors
   * @param ncores     cores per executor
   * @param maxram     maxram
   * @return start a new spark context
   */
  def startSparkSession(appName: String = "CTM_test", nexecutors: Int = NEXECUTORS, ncores: Int = NCORES, maxram: String = MAXRAM,
                        shufflepartitions: Int = SPARK_SQL_TEST_SHUFFLE_PARTITIONS, master: String = "local[*]"): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.shuffle.reduceLocality.enabled", value = false)
      .config("spark.executor.instances", nexecutors)
      .config("spark.executor.cores", ncores)
      .config("spark.executor.memory", maxram)
      .config("spark.sql.shuffle.partitions", shufflepartitions)
      // https://kb.databricks.com/jobs/spark-overwrite-cancel.html
      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      .enableHiveSupport
      .getOrCreate
}
