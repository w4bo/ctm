package it.unibo.big

import it.unibo.big.TemporalScale.AbsoluteScale
import it.unibo.big.TestRegression.sparkSession
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeAll, Test}
import org.roaringbitmap.RoaringBitmap

object TestRegression {
    @transient var sparkSession: SparkSession = _

    @BeforeAll def beforeAll(): Unit = {
        sparkSession = TestPaper.startSparkTestSession()
    }
}

class TestRegression {

    @Test def testRegression(): Unit = {
        var inTable: String = "trajectory.oldenburg_standard_2000_distinct"
        val minsize: Int = 4
        val minsup: Int = 5
        val bin_s: Int = 20
        val limit: Int = 50 // 100
        val timescale: TemporalScale = AbsoluteScale
        val bin_t: Int = 1
        val euclidean: Boolean = true

        var res = CTM.run(returnResult = true, spark = Some(sparkSession), inTable = inTable, limit = limit, minsize = minsize, minsup = minsup, bin_s = bin_s, bin_t = bin_t, timeScale = timescale, euclidean = euclidean)
        assertEquals(4, res._1)
        // CTM.run(spark = Some(sparkSession), storage_thr = 1000000, droptable = true, inTable = inTable, limit = limit, minsize = minsize, minsup = minsup, bin_s = bin_s, bin_t = bin_t, timeScale = timescale, euclidean = euclidean)
        // Query.run(inTable.replace("trajectory.", ""), minsize, minsup, bin_s, timescale, bin_t, euclidean, "export", limit)

        inTable = "ctm.join__oldenburg_standard_2000_distinct__4__5__20__absolute__1"
        res = CTM.run(returnResult = true, spark = Some(sparkSession), inTable = inTable, limit = limit, minsize = minsize, minsup = minsup, bin_s = bin_s, bin_t = bin_t, timeScale = timescale, euclidean = euclidean)
        assertEquals(4, res._1)
    }

    @Test def testRegression1(): Unit = {
        val inTable: String = "trajectory.oldenburg_standard_2000_distinct"
        val minsize: Int = 10
        val minsup: Int = 34
        val bin_s: Int = 20
        val limit: Int = 1000000
        val timescale: TemporalScale = AbsoluteScale
        val bin_t: Int = 1
        val euclidean: Boolean = true

        CTM.run(droptable = true, spark = Some(sparkSession), storage_thr = 1000000, inTable = inTable, limit = limit, minsize = minsize, minsup = minsup, bin_s = bin_s, bin_t = bin_t, timeScale = timescale, euclidean = euclidean)
        Query.run(inTable.replace("trajectory.", ""), minsize, minsup, bin_s, timescale, bin_t, euclidean, "export", limit)
        
        val source = scala.io.Source.fromFile("itemsets.txt")
        val lines: String = try source.mkString finally source.close()
        val spareItemsets: Set[Set[Int]] = lines.replace("{", "").replace("}", "").split("\n").map(l => l.split(", ").map(_.toInt).toSet).toSet

        val res: (Long, Array[(RoaringBitmap, Int, Int)]) = CTM.run(returnResult = true, spark = Some(sparkSession), inTable = inTable, limit = limit, minsize = minsize, minsup = minsup, bin_s = bin_s, bin_t = bin_t, timeScale = timescale, euclidean = euclidean)
        var ctmItemsets: Set[Set[Int]] = res._2.map(_._1.toArray.toSet).toSet
        println(res._2.toSet)

        spareItemsets.foreach(i => ctmItemsets -= i)
        // assertEquals(Set(), ctmItemsets)
    }
}