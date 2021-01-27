package it.unibo.big

import it.unibo.big.TemporalScale.{AbsoluteScale, NoScale}
import it.unibo.big.Utils.{Itemid, Tid}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.roaringbitmap.RoaringBitmap
import org.scalatest._

import scala.collection.mutable

class TestPaper extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  @transient var sparkSession: SparkSession = _

  /*  Cell indexes withing the tessellation
   *  1 -  2 -  3 -  4 -  5 -  6 -  7
   *  8 -  9 - 10 - 11 - 12 - 13 - 14
   * 15 - 16 - 17 - 18 - 19 - 20 - 21
   */

  /**
   * Create a Tessellation
   *
   * @param rows    number or rows
   * @param cols    number of columns
   * @param distCol max distance between columns
   * @param distRow max distance between columns
   * @param symmetric a is neighbor of b, but not viceversa
   * @return
   */
  def neigh(rows: Int, cols: Int, distCol: Int, distRow: Int, symmetric: Boolean = true): Map[Int, RoaringBitmap] = {
    val ret: mutable.Map[Int, RoaringBitmap] = mutable.Map()
    (0 until rows).foreach(s => {
      (1 to cols).foreach(t => {
        var curMap = RoaringBitmap.bitmapOf()
        (0 until rows).foreach(remainingSpace => {
          (1 to cols).foreach(remainingTime => {
            val ds = Math.abs(s - remainingSpace)
            val dt = Math.abs(t - remainingTime)
            val curCell = s * cols + t
            val neighCell = remainingSpace * cols + remainingTime
            if ((!symmetric && neighCell > curCell || symmetric && neighCell != curCell) && ds <= distRow && dt <= distCol) {
              curMap = RoaringBitmap.or(curMap, RoaringBitmap.bitmapOf(remainingSpace * cols + remainingTime))
            }
          })
        })
        ret.put(s * cols + t, curMap)
      })
    })
    ret.toMap
  }

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .master("local[1]") // Delete this if run in cluster mode
      .appName("TestPaper") // Change this to a proper name
      .config("spark.broadcast.compress", "false")
      .config("spark.shuffle.compress", "false")
      .config("spark.shuffle.spill.compress", "false")
      .config("spark.io.compression.codec", "lzf")
      .getOrCreate()
    GeoSparkSQLRegistrator.registerAll(sparkSession)
    sparkSession.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }

  override def afterAll(): Unit = {
    sparkSession.sparkContext.stop()
  }

  test("test neighborhood generation") {
    assert(neigh(3, 7, 1, 1, false)(1).getCardinality == 3, neigh(3, 7, 1, 1)(1).toString)
    assert(neigh(3, 7, 1, Int.MaxValue, false)(2).getCardinality == 7, "Wrong number of neighbors")
    assert(neigh(3, 7, Int.MaxValue, Int.MaxValue, false).size == 21, "Wrong number of transactions")
    assert(neigh(3, 7, Int.MaxValue, Int.MaxValue, false)(1).getCardinality == 20, "Wrong number of neighbors")
    assert(neigh(3, 7, Int.MaxValue, Int.MaxValue, false)(15).getCardinality == 6, "Wrong number of neighbors")
    assert(neigh(3, 7, 1, Int.MaxValue, false)(1).getCardinality == 5, neigh(3, 7, 1, Int.MaxValue, false)(1).toString)
    assert(neigh(3, 7, 2, Int.MaxValue, false)(2).getCardinality == 10, neigh(3, 7, 2, Int.MaxValue, false)(2).toString)
    assert(neigh(3, 7, 1, Int.MaxValue, true)(10).getCardinality == 8, neigh(3, 7, 1, Int.MaxValue, true)(9).toString)
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
    val data12: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2, 3)),
      (2, Vector(1, 2, 3)),
      (3, Vector(1, 2, 3)))
    val res12 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 1, debugData = data12, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res12._1 == 1, "Test 12 failed")
    assert(res12._2.toSet.equals(Set((RoaringBitmap.bitmapOf(1, 2, 3), 3, 3))), "Test 12 failed")
  }

  test("6") {
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
    val data11: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2, 3)),
      (2, Vector(1, 2, 3)),
      (3, Vector(2, 3)))
    val res11 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 1, debugData = data11, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res11._1 == 2, "Test 11 failed")
    assert(res11._2.toSet.equals(Set((RoaringBitmap.bitmapOf(2, 3), 2, 3), (RoaringBitmap.bitmapOf(1, 2, 3), 3, 2))), "Test 11 failed")
  }

  test("8") {
    val data10: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2, 3)),
      (2, Vector(2, 3)),
      (3, Vector(1, 2, 3)))
    val res10 = CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 1, debugData = data10, bin_s = 1, timeScale = NoScale, returnResult = true)
    assert(res10._1 == 2, "Test 10 failed")
    assert(res10._2.toSet.equals(Set((RoaringBitmap.bitmapOf(2, 3), 2, 3), (RoaringBitmap.bitmapOf(1, 2, 3), 3, 2))), "Test 10 failed")
  }

  test("9") {
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

    val res = CTM.run(spark = Some(sparkSession),
      minsize = 2,
      minsup = 2,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      returnResult = true,
      debugData = data,
      neighs = neigh(3, 7, Int.MaxValue, Int.MaxValue, true)
    )

    assert(res._1 == 1, "Test 10 failed")
    assert(res._2.toSet.equals(Set((RoaringBitmap.bitmapOf(1, 2), 2, 2))), res._2.toString)
  }

  test("Flow") {
    val data: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1)),
      (3, Vector(1)),
      (4, Vector(1)),
      (5, Vector(1)),
      (7, Vector(1)),
      (8, Vector(2)),
      (9, Vector(1, 2)),
      (13, Vector(1, 2)),
      (15, Vector(3)),
      (16, Vector(3)),
      (17, Vector(2, 3)),
      (18, Vector(2, 3)),
      (19, Vector(2, 3)),
      (20, Vector(3)),
      (21, Vector(3))
    )
    val res = CTM.run(spark = Some(sparkSession),
      minsize = 2,
      minsup = 3,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      returnResult = true,
      debugData = data,
      neighs = neigh(3, 7, Int.MaxValue, 1, true)
    )
    assert(res._1 == 1, "Flow failed")
    assert(res._2.toSet.equals(Set((RoaringBitmap.bitmapOf(2, 3), 2, 3))), res._2.toSet.toString)
  }

  test("Swarm") {
    val data: Seq[(Tid, Vector[Itemid])] = Vector(
      (5, Vector(1)),
      (7, Vector(1)),
      (8, Vector(1, 2)),
      (9, Vector(1, 2, 3)),
      (10, Vector(1, 2)),
      (11, Vector(1, 2)),
      (12, Vector(2, 3)),
      (13, Vector(1, 2, 3)),
      (14, Vector(1, 2)),
      (15, Vector(3)),
      (17, Vector(3)),
      (18, Vector(3)),
      (21, Vector(3))
    )
    val res = CTM.run(spark = Some(sparkSession),
      minsize = 3,
      minsup = 2,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      returnResult = true,
      debugData = data,
      neighs = neigh(3, 7, Int.MaxValue, Int.MaxValue, true)
    )
    assert(res._2.toSet.equals(Set((RoaringBitmap.bitmapOf(1, 2, 3), 3, 2))), res._2.toSet.toString)
  }

  test("Platoon") {
    val data: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2)),
      (3, Vector(1, 2)),
      (4, Vector(1)),
      (7, Vector(1, 2)),
      (8, Vector(1, 2)),
      (11, Vector(2)),
      (12, Vector(1)),
      (16, Vector(1, 2)),
      (19, Vector(3)),
      (20, Vector(1, 2))
    )
    val res = CTM.run(spark = Some(sparkSession),
      minsize = 2,
      minsup = 2,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      returnResult = true,
      debugData = data,
      neighs = neigh(3, 7, 1, Int.MaxValue, true),
      platoon = true
    )
    assert(res._1 == 1, "Failed, current result is: " + res)
    assert(res._2.toSet.equals(Set((RoaringBitmap.bitmapOf(1, 2), 2, 6))), res._2.toSet.toString)
  }

  test("Platoon 2") {
    val data: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2, 3, 4, 5, 6)),
      (2, Vector(1, 2, 3)),
      (4, Vector(1, 2, 4)),
      (5, Vector(1, 2, 5)),
      (7, Vector(1, 2, 6))
    )
    val res = CTM.run(spark = Some(sparkSession),
      minsize = 2,
      minsup = 2,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      returnResult = true,
      debugData = data,
      neighs = neigh(3, 7, 1, Int.MaxValue, true),
      platoon = true
    )
    assert(res._2.toSet.equals(Set((RoaringBitmap.bitmapOf(1, 2, 3), 3, 2))), res._2.toSet.toString)
  }

  test("Platoon 3") {
    val data: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2, 3, 4, 5, 6)),
      (2, Vector(1, 2, 3)),
      (4, Vector(1, 2, 4)),
      (5, Vector(1, 2, 5)),
      (7, Vector(1, 6))
    )
    val res = CTM.run(spark = Some(sparkSession),
      minsize = 2,
      minsup = 2,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      returnResult = true,
      debugData = data,
      neighs = neigh(3, 7, 1, Int.MaxValue, true),
      platoon = true
    )
    assert(res._2.toSet.equals(Set((RoaringBitmap.bitmapOf(1, 2, 3), 3, 2), (RoaringBitmap.bitmapOf(1, 2), 2, 4))), res._2.toSet.toString)
  }

  test("Platoon fail") {
    val data: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2)),
      (3, Vector(1, 2)),
      (4, Vector(1)),
      (7, Vector(1, 2)),
      (8, Vector(1, 2)),
      (11, Vector(2)),
      (12, Vector(1)),
      (16, Vector(1, 2)),
      (19, Vector(3)),
      (20, Vector(1, 2))
    )
    val res = CTM.run(spark = Some(sparkSession),
      minsize = 2,
      minsup = 3,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      returnResult = true,
      debugData = data,
      neighs = neigh(3, 7, 1, Int.MaxValue, true),
      platoon = true
    )
    assert(res._2.toSet.equals(Set()), res._2.toSet.toString)
  }

  test("Flock") {
    val data: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2)),
      (3, Vector(1, 2)),
      (4, Vector(1)),
      (7, Vector(1, 2)),
      (8, Vector(1, 2)),
      (11, Vector(2)),
      (12, Vector(1)),
      (16, Vector(1, 2)),
      (19, Vector(3)),
      (20, Vector(1, 2))
    )
    val res = CTM.run(spark = Some(sparkSession),
      minsize = 2,
      minsup = 3,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      returnResult = true,
      debugData = data,
      neighs = neigh(3, 7, 1, Int.MaxValue, true)
    )
    assert(res._1 == 1, "Failed, current result is: " + res)
    assert(res._2.toSet.equals(Set((RoaringBitmap.bitmapOf(1, 2), 2, 6))), res._2.toSet.toString)
  }

  test("Flock fail") {
    val data = Vector(
      (1, Vector(1, 2)),
      (3, Vector(1, 2)),
      (4, Vector(1)),
      (7, Vector(1, 2)),
      (11, Vector(2)),
      (12, Vector(1)),
      (16, Vector(1, 2)),
      (19, Vector(3)),
      (20, Vector(1, 2))
    )
    val res = CTM.run(spark = Some(sparkSession),
      minsize = 2,
      minsup = 4,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      returnResult = true,
      debugData = data,
      neighs = neigh(3, 7, 1, Int.MaxValue, true)
    )
    assert(res._1 == 0, "Flock failed, result should be empty but is: " + res)
  }

  test("Flock 2") {
    val data: Seq[(Tid, Vector[Itemid])] = Vector(
      (1, Vector(1, 2, 3, 4)),
      (2, Vector(1, 2, 4)),
      (3, Vector(1, 2, 3))
    )
    val res = CTM.run(spark = Some(sparkSession),
      minsize = 2,
      minsup = 3,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      returnResult = true,
      debugData = data,
      neighs = neigh(3, 7, 1, Int.MaxValue, true)
    )
    assert(res._2.toSet.equals(Set((RoaringBitmap.bitmapOf(1, 2), 2, 3))), res._2.toSet.toString)
  }
}