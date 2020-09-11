package it.unibo.big

import it.unibo.big.TemporalScale.{AbsoluteScale, NoScale}
import it.unibo.big.Utils.{Itemid, Tid}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.roaringbitmap.RoaringBitmap
import org.scalatest._

import scala.collection.mutable

class TestPaper extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  @transient var sparkSession: SparkSession = _
  @transient var sc: SparkContext = _

  /*  Cell locations
   *  1 -  2 -  3 -  4 -  5 -  6 -  7
   *  8 -  9 - 10 - 11 - 12 - 13 - 14
   * 15 - 16 - 17 - 18 - 19 - 20 - 21
   */

  def neigh(n_s: Int, n_t: Int, max_t: Int, max_s: Int): Map[Int, RoaringBitmap] = {
    val ret: mutable.Map[Int, RoaringBitmap] = mutable.Map()
    (0 until n_s).foreach(s => {
      (1 to n_t).foreach(t => {
        var curMap = RoaringBitmap.bitmapOf()
        (0 until n_s).foreach(remainingSpace => {
          (1 to n_t).foreach(remainingTime => {
            val ds = Math.abs(s - remainingSpace)
            val dt = Math.abs(t - remainingTime)
            val curcell = s * n_t + t
            val neigh = remainingSpace * n_t + remainingTime
            if (neigh > curcell && ds <= max_s && dt <= max_t) {
              curMap = RoaringBitmap.or(curMap, RoaringBitmap.bitmapOf(remainingSpace * n_t + remainingTime))
            }
          })
        })
        ret.put(s * n_t + t, curMap)
      })
    })
    ret.toMap
  }
  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .master("local[2]") // Delete this if run in cluster mode
      .appName("unit test") // Change this to a proper name
      .config("spark.broadcast.compress", "false")
      .config("spark.shuffle.compress", "false")
      .config("spark.shuffle.spill.compress", "false")
      .config("spark.io.compression.codec", "lzf")
      .getOrCreate()
    GeoSparkSQLRegistrator.registerAll(sparkSession)
    sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("test neighborhood generation") {
    assert(neigh(3, 7, 1, 1)(1).getCardinality == 3, neigh(3, 7, 1, 1)(1).toString)
    assert(neigh(3, 7, 1, 10)(2).getCardinality == 7, "Wrong number of neighbors")
    assert(neigh(3, 7, 10, 10).size == 21, "Wrong number of transactions")
    assert(neigh(3, 7, 10, 10)(1).getCardinality == 20, "Wrong number of neighbors")
    assert(neigh(3, 7, 10, 10)(15).getCardinality == 6, "Wrong number of neighbors")
    assert(neigh(3, 7, 1, 10)(1).getCardinality == 5, neigh(3, 7, 1, 1)(1).toString)
    assert(neigh(3, 7, 2, 10)(2).getCardinality == 10, neigh(3, 7, 1, 1)(2).toString)
  }

  test("1") {
    val data17: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(0, 1, 2)),
      (2, Vector(0, 1)),
      (3, Vector(1, 2)),
      (4, Vector(1, 2)),
      (5, Vector(0, 2)),
      (6, Vector(0, 2)),
      (7, Vector(0, 2)),
      (8, Vector(1)),
      (9, Vector(1)),
      (10, Vector(2)))
    val res17 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 1, debugData = data17, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res17._1 == 7, "Test 17 failed")
  }

  test("2") {
    /* -- Test 16 --------------------------------------------------------------------------------------------------- */
    val data16: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(0, 1, 2)),
      (2, Vector(0, 1)),
      (3, Vector(0, 2)),
      (4, Vector(0, 2)),
      (5, Vector(0, 2)),
      (6, Vector(1)),
      (7, Vector(1)),
      (8, Vector(2)))
    val res16 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 1, debugData = data16, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res16._1 == 6, "Test 16 failed")
  }

  test("3") {
    /* -- Test 15 --------------------------------------------------------------------------------------------------- */
    val data15: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(0, 1, 2)),
      (2, Vector(0, 1)),
      (3, Vector(0, 2)),
      (4, Vector(0, 2)),
      (5, Vector(0, 2)),
      (6, Vector(2)))
    val res15 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 1, debugData = data15, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res15._1 == 5, "Test 15 failed")
  }

  test("4") {
    /* -- Test 14 --------------------------------------------------------------------------------------------------- */
    val data14: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2, 3, 4, 5)),
      (2, Vector(2, 3, 4, 6)),
      (3, Vector(2, 3, 4, 7)),
      (4, Vector(8, 9)))
    val res14 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 3, debugData = data14, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res14._1 == 1, "Test 14 failed")
    assert(res14._2.toSet.equals(Set((RoaringBitmap.bitmapOf(2, 3, 4), 3, 3))), "Test 14 failed")
  }

  test("5") {
    /* -- Test 12 --------------------------------------------------------------------------------------------------- */
    val data12: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2, 3)),
      (2, Vector(1, 2, 3)),
      (3, Vector(1, 2, 3)))
    val res12 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 1, debugData = data12, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res12._1 == 1, "Test 12 failed")
    assert(res12._2.toSet.equals(Set((RoaringBitmap.bitmapOf(1, 2, 3), 3, 3))), "Test 12 failed")
  }

  test("6") {
    /* -- Test 13 --------------------------------------------------------------------------------------------------- */
    val data13: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2, 3)),
      (2, Vector(2, 3, 4)),
      (3, Vector(2, 3, 5)),
      (4, Vector(2, 3, 6)))
    val res13 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 2, debugData = data13, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res13._1 == 1, "Test 13 failed")
    assert(res13._2.toSet.equals(Set((RoaringBitmap.bitmapOf(2, 3), 2, 4))), "Test 13 failed")
  }

  test("7") {
    /* -- Test 11 --------------------------------------------------------------------------------------------------- */
    val data11: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2, 3)),
      (2, Vector(1, 2, 3)),
      (3, Vector(2, 3)))
    val res11 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 1, debugData = data11, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res11._1 == 2, "Test 11 failed")
    assert(res11._2.toSet.equals(Set((RoaringBitmap.bitmapOf(2, 3), 2, 3), (RoaringBitmap.bitmapOf(1, 2, 3), 3, 2))), "Test 11 failed")
  }

  test("8") {
    /* -- Test 10 --------------------------------------------------------------------------------------------------- */
    val data10: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2, 3)),
      (2, Vector(2, 3)),
      (3, Vector(1, 2, 3)))
    val res10 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 1, debugData = data10, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res10._1 == 2, "Test 10 failed")
    assert(res10._2.toSet.equals(Set((RoaringBitmap.bitmapOf(2, 3), 2, 3), (RoaringBitmap.bitmapOf(1, 2, 3), 3, 2))), "Test 10 failed")
  }

  test("9") {
    /* -- Test 08 --------------------------------------------------------------------------------------------------- */
    val data8: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 3, 4)),
      (2, Vector(2, 3, 5)),
      (3, Vector(1, 2, 3, 5)),
      (4, Vector(2, 5)),
      (5, Vector(1, 2, 3, 5)))
    val res8 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 1, debugData = data8, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res8._1 == 6, "Test 8 failed")
    assert(res8._2.toSet.equals(Set(
      (RoaringBitmap.bitmapOf(3), 1, 4),
      (RoaringBitmap.bitmapOf(1, 3), 2, 3),
      (RoaringBitmap.bitmapOf(1, 3, 4), 3, 1),
      (RoaringBitmap.bitmapOf(2, 5), 2, 4),
      (RoaringBitmap.bitmapOf(2, 3, 5), 3, 3),
      (RoaringBitmap.bitmapOf(1, 2, 3, 5), 4, 2))
    ), "Test 8 failed")
  }

  test("Co-location") {

    val data: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1)),
      (8, Vector(2)),
      (9, Vector(1, 2)),
      (13, Vector(1, 2)),
      (3, Vector(1)),
      (4, Vector(1)),
      (5, Vector(1)),
      (6, Vector(1)),
      (17, Vector(2)),
      (18, Vector(2)),
      (19, Vector(2)),
      (14, Vector(2))
    )

    val res: (Long, Array[(RoaringBitmap, Int, Int)]) = CTM.run(spark = Some(sparkSession),
      minsize = 2,
      minsup = 2,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      returnResult = true,
      debugData = data,
      neighs = neigh(3, 7, 10, 10)
    )

    assert(res._1 == 1, "Test 10 failed")
    assert(res._2.toSet.equals(Set((RoaringBitmap.bitmapOf(1, 2), 2, 2))), res._2.toString)
  }
}