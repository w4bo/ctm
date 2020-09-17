package it.unibo.big

import java.util.Random

import it.unibo.big.TemporalScale.{AbsoluteScale, NoScale}
import it.unibo.big.Utils.{Itemid, Tid}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.roaringbitmap.RoaringBitmap
import org.scalatest._

import scala.collection.mutable

class TestScalability extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  @transient var sparkSession: SparkSession = _

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

//  test("scalability") {
//    val r: Random = new Random(3)
//    val data: Seq[(Int, Vector[Int])] = (1 to 21).map(tid => (tid, (1 to 100000).filter(_ => r.nextDouble() >= 0.5).toVector))
//    CTM.run(spark = Some(sparkSession), minsize = 1, minsup = 1, debugData = data, bin_s = 1, timeScale = NoScale, returnResult = true)
//  }
//
//  test("support") {
//    val r: Random = new Random(3)
//    val data: Map[Int, RoaringBitmap] = (1 to 100).map(tid => (tid, RoaringBitmap.bitmapOf((1 to 1000000).filter(_ => r.nextDouble() >= 0.5).toVector: _*))).toMap
//
//    def support1(itemset: RoaringBitmap): RoaringBitmap = {
//      RoaringBitmap.bitmapOf(data.filter({ case (_, transaction) =>
//        val iterator = itemset.iterator()
//        var isOk = true
//        while (isOk && iterator.hasNext) {
//          isOk = transaction.contains(iterator.next())
//        }
//        isOk
//      }).keys.toSeq: _*)
//    }
//
//    def support2(itemset: RoaringBitmap): RoaringBitmap = {
//      val res = RoaringBitmap.bitmapOf()
//      var first = -1
//      var cur = -1
//      data.foreach({ case (tid, transaction) =>
//        val iterator = itemset.iterator()
//        var isOk = true
//        while (isOk && iterator.hasNext) {
//          isOk = transaction.contains(iterator.next())
//        }
//        if (isOk) {
//          if (first < 0) {
//            first = tid
//            cur = tid
//          }
//          if (tid - cur > 1) {
//            res.add(first, cur)
//            first = tid
//          }
//          cur = tid
//        }
//      })
//      res.add(first, cur)
//      res
//    }
//
//    def supportOr(itemset: RoaringBitmap): RoaringBitmap = {
//      RoaringBitmap.bitmapOf(data.filter({ case (_, transaction) =>
//        RoaringBitmap.or(transaction, itemset).getCardinality == transaction.getCardinality
//      }).keys.toSeq: _*)
//    }
//
//    def supportAnd(itemset: RoaringBitmap): RoaringBitmap = {
//      RoaringBitmap.bitmapOf(data.filter({ case (_, transaction) =>
//        RoaringBitmap.and(transaction, itemset).getCardinality == itemset.getCardinality
//      }).keys.toSeq: _*)
//    }
//
//    var startTime: Long = System.currentTimeMillis()
//
//    val n = 3
//    val acc: mutable.Map[String, Long] = mutable.Map()
//    (0 to 10).foreach(run => {
//      println("Run: " + run)
//      val itemset: Seq[RoaringBitmap] = (1 to 100).map(tid => RoaringBitmap.bitmapOf((1 to 1000000).filter(_ => r.nextDouble() >= run / 10.0).toVector: _*))
//
//      startTime = System.currentTimeMillis()
//      itemset.foreach(support1)
//      acc.put("support1", acc.getOrElse("support1", 0L) + System.currentTimeMillis() - startTime)
//
//      startTime = System.currentTimeMillis()
//      itemset.foreach(support2)
//      acc.put("support2", acc.getOrElse("support2", 0L) + System.currentTimeMillis() - startTime)
//
//      startTime = System.currentTimeMillis()
//      itemset.foreach(supportOr)
//      acc.put("supportOr", acc.getOrElse("supportOr", 0L) + System.currentTimeMillis() - startTime)
//
//      startTime = System.currentTimeMillis()
//      itemset.foreach(supportAnd)
//      acc.put("supportAnd", acc.getOrElse("supportAnd", 0L) + System.currentTimeMillis() - startTime)
//    })
//    acc.foreach(t => println(s"${t._1}: ${t._2 / n}"))
//  }
}