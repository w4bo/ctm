package it.unibo.big

import it.unibo.big.TestOnCluster.sparkSession
import it.unibo.big.temporal.TemporalScale.{AbsoluteScale, DailyScale, NoScale, WeeklyScale}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeAll, Test}
import org.roaringbitmap.RoaringBitmap

object TestOnCluster {
    @transient var sparkSession: SparkSession = _

    @BeforeAll def beforeAll(): Unit = {
        sparkSession = TestPaper.startSparkTestSession()
    }
}

class TestOnCluster {
    type cuteCluster = (RoaringBitmap, Int, Int)

    def loadAndStoreDataset(fileLines: Array[String], tempTableName: String, spark: SparkSession): Unit = {
        val inputRDD =
            spark
                .sparkContext
                .parallelize(fileLines)
                .map(_.split(Utils.FIELD_SEPARATOR))
                .map(e => Row(e(0), e(0), e(1).toDouble, e(2).toDouble, e(3).toLong))
        spark.createDataFrame(inputRDD, Utils.INPUT_REQUIRED_SCHEMA).createOrReplaceTempView(tempTableName)
    }

    @Test def testDB(): Unit = {
        var res9 = Main.run(spark = Some(sparkSession), droptable = true, minsize = 1, minsup = 25, bin_s = 10, inTable = "trajectory.besttrj_standard", timeScale = NoScale, returnResult = true)
        assertEquals(2466, res9._2.length)
        res9 = Main.run(spark = Some(sparkSession), droptable = false, minsize = 1, minsup = 25, bin_s = 10, inTable = "trajectory.besttrj_standard", timeScale = NoScale)
        assertEquals(2466, res9._1)
        assertTrue(2466 > res9._2.length)
    }

    //    @Test def testDB3(): Unit = {
    //        def setup(sup: Int): (Long, Array[(RoaringBitmap, Int, Int)], Long) = CTM.run(spark = Some(sparkSession), droptable = true, minsize = 100, minsup = sup, bin_s = 16, bin_t = 4, timeScale = DailyScale, inTable = "trajectory.milan_standard", returnResult = true)
    //        var sup = 15
    //        var continue = true
    //        var prevData = setup(sup)
    //        while (continue) {
    //            println("\n\n----- SUP: " + sup + "\n\n")
    //            continue &= prevData._2.nonEmpty
    //            val newData = setup(sup + 1)
    //            assertTrue(prevData._1 >= newData._1, s"Itemset should increase for lower support. prev: ${prevData._1}, new: ${newData._1}")
    //            assertEquals(prevData._2.length, prevData._2.map(i => i._1).toSet.size, "All the itemsets should be diverse")
    //            assertEquals(newData._2.length, newData._2.map(i => i._1).toSet.size, "All the itemsets should be diverse")
    //            assertEquals(Set(), newData._2.map(i => i._1).toSet.diff(prevData._2.map(i => i._1).toSet), s"The lower support RDD does not contains some of the higher support RDD's itemset\npre: ${prevData._2.sortBy(-_._2).map(i => i._1).toSeq}\nnew: ${newData._2.sortBy(-_._2).map(i => i._1).toSeq}")
    //            assertTrue(prevData._3 >= newData._3, s"The enumerated space should not decrease. prev: ${prevData._3}, new: ${newData._3}")
    //            prevData = newData
    //            sup += 1
    //        }
    //    }

    /**
     * Test a pattern where the trajectories are in three near cells.
     */
    @Test def testAbsoluteContiguityClusters(): Unit = {
        val test_name = "AbsoluteContiguityClusters"
        val absoluteContiguityInputSet: Array[String] =
            Array(
                "01\t0\t0\t1", //
                "01\t0\t0\t2", //
                "01\t0\t0\t3", //
                "02\t0\t0\t1", //
                "02\t0\t0\t2", //
                "02\t0\t0\t3")
        val absoluteContiguityTableView = "simple_table_view"
        loadAndStoreDataset(absoluteContiguityInputSet, absoluteContiguityTableView, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession), droptable = true, inTable = absoluteContiguityTableView, minsize = 2, minsup = 2, bin_s = 1, timeScale = AbsoluteScale, bin_t = 1, eps_t = 1, returnResult = true)
        val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 3))
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }

    /**
     * Test a pattern where the trajectories are in three contigued cells.
     */
    @Test def testAbsoluteContiguityClustersNOResult(): Unit = {
        val absoluteContiguityInputSet: Array[String] = Array("01\t0\t0\t1", "01\t0\t0\t2", "01\t0\t0\t3", "02\t0\t0\t1", "02\t0\t0\t2", "02\t0\t0\t4")
        val absoluteContiguityTableView = "simple_table_view_NOresult"
        loadAndStoreDataset(absoluteContiguityInputSet, absoluteContiguityTableView, sparkSession)
        val absoluteContiguityClusters = Main.run(spark = Some(sparkSession), droptable = true, inTable = absoluteContiguityTableView, minsize = 2, minsup = 3, bin_s = 1, timeScale = AbsoluteScale, bin_t = 1, eps_t = 1, returnResult = true)
        assertTrue(absoluteContiguityClusters._2.isEmpty)
    }

    /** Check that contiguity works also with relaxed time constrains */
    @Test def testSmootherContiguityClusters(): Unit = {
        val test_name = "SMOOTHER_CONTIGUITY_CHECK"
        val inputSet: Array[String] = Array( //
            "01\t0\t0\t1", "01\t0\t0\t2", "01\t0\t0\t4", "01\t0\t0\t6", //
            "02\t0\t0\t1", "02\t0\t0\t2", "02\t0\t0\t4", "02\t0\t0\t6")
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession), droptable = true, inTable = tableName, minsize = 2, minsup = 3, bin_s = 1, timeScale = AbsoluteScale, bin_t = 1, eps_t = 2, returnResult = true)
        val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 4))
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }

    /** Check that contiguity works also with relaxed time constrains. */
    @Test def testSmootherContiguityTwoThresholdClusters(): Unit = {
        val test_name = "SMOOTHER_CONTIGUITY_TWO_CHECK"
        val inputSet: Array[String] =
            Array( //
                "01\t0\t0\t1", //
                "01\t0\t0\t2", //
                "01\t0\t0\t5", //
                "01\t0\t0\t7", //
                "02\t0\t0\t1", //
                "02\t0\t0\t2", //
                "02\t0\t0\t5", //
                "02\t0\t0\t7") //
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession), droptable = true, inTable = tableName, minsize = 2, minsup = 2, bin_s = 1, timeScale = AbsoluteScale, bin_t = 1, eps_t = 2, returnResult = true)
        val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 4))
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }

    /**
     * Check that contiguity works also with relaxed time costrains, no result should be found here.
     */
    @Test def testSmootherContiguityClustersNOResult(): Unit = {
        val test_name = "SMOOTHER_CONTIGUITY_CHECK_NO_RESULT"
        val inputSet: Array[String] = Array( //
            "01\t0\t0\t1", "01\t0\t0\t2", "01\t0\t0\t5", "01\t0\t0\t7", //
            "02\t0\t0\t1", "02\t0\t0\t2", "02\t0\t0\t5", "02\t0\t0\t7")
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession),
            droptable = true,
            inTable = tableName,
            minsize = 2,
            minsup = 3,
            bin_s = 1,
            timeScale = AbsoluteScale,
            bin_t = 1,
            eps_t = 2,
            returnResult = true
        )
        assertTrue(cuteClusters._2.isEmpty)
    }

    /** This test include a cell with an superior ID that contains the pattern required but is excluded for some spatio-temporal reason. */
    @Test def testExternalNeighbourClusters(): Unit = {
        val test_name = "EXTERNAL_NEIGHBOUR_CHECK"
        val inputSet: Array[String] = Array( //
            "01\t10\t10\t1", "01\t0\t0\t3", "01\t0\t0\t4", "01\t0\t0\t5", //
            "02\t10\t10\t1", "02\t0\t0\t3", "02\t0\t0\t4", "02\t0\t0\t5")
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession),
            droptable = true,
            inTable = tableName,
            minsize = 2,
            minsup = 2,
            bin_s = 1,
            timeScale = AbsoluteScale,
            bin_t = 1,
            eps_t = 1,
            returnResult = true
        )
        val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 4))
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }

    /**
     * This test include a cell with an superior ID that contains the pattern required but is excluded for some
     * spatio-temporal reason.
     */
    @Test def testExternalNeighbourInsideTheIDPathClusters(): Unit = {
        val test_name = "EXTERNAL_PATH_NEIGHBOUR_CHECK"
        val inputSet: Array[String] = Array( //
            "01\t10\t10\t1", "01\t0\t0\t3", "01\t10\t10\t4", "01\t10\t10\t5", //
            "02\t10\t10\t1", "02\t0\t0\t3", "02\t10\t10\t4", "02\t10\t10\t5")
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession),
            droptable = true,
            inTable = tableName,
            minsize = 2,
            minsup = 2,
            bin_s = 1,
            timeScale = AbsoluteScale,
            bin_t = 1,
            eps_t = 1,
            returnResult = true
        )
        val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 4))
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }

    /**
     * This test include a cell with an superior ID that contains the pattern required but is excluded for some
     * spatio-temporal reason.
     */
    @Test def testTwoSplitsClusters(): Unit = {
        val test_name = "TWO_SPLITS_CHECK"
        val inputSet: Array[String] = Array(
            "01\t0\t0\t1", //
            "01\t0\t0\t2", //
            "01\t0\t0\t4", //
            "01\t0\t0\t5", //
            "01\t0\t0\t7", //
            "01\t0\t0\t8", //
            "02\t0\t0\t1", //
            "02\t0\t0\t2", //
            "02\t0\t0\t4", //
            "02\t0\t0\t5", //
            "02\t0\t0\t7", //
            "02\t0\t0\t8")
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession), droptable = true, inTable = tableName, minsize = 2, minsup = 2, bin_s = 1, timeScale = AbsoluteScale, bin_t = 1, eps_t = 1, returnResult = true)
        val expectedClusters = Set(
            (RoaringBitmap.bitmapOf(0, 1), 2, 6)
            // (RoaringBitmap.bitmapOf(1, 2), 2),
            // (RoaringBitmap.bitmapOf(1, 2), 2),
            // (RoaringBitmap.bitmapOf(1, 2), 2)
        )
        assertEquals(expectedClusters, cuteClusters._2.toSet)
        // println(s"----$test_name: PASSED------")
    }

    /**
     * Test the recognition of a Swarm pattern.
     */
    @Test def testSwarmDetection(): Unit = {
        val test_name = "SWARM_DETECTION"
        val inputSet: Array[String] = Array(
            "01\t00\t00\t1", "01\t00\t00\t2", "01\t00\t00\t3", "01\t00\t00\t4", "01\t00\t00\t5", "01\t00\t00\t6",
            "02\t00\t00\t1", "02\t00\t00\t2", "02\t10\t10\t3", "02\t00\t00\t4", "02\t00\t00\t5", "02\t05\t05\t6",
            "03\t10\t10\t1", "03\t10\t10\t2", "03\t10\t10\t3", "03\t00\t00\t4", "03\t05\t05\t5", "03\t05\t05\t6",
            "04\t10\t10\t1", "04\t10\t10\t2", "04\t10\t10\t3", "04\t10\t10\t4", "04\t20\t20\t5", "04\t15\t15\t6",
            "05\t20\t20\t1", "05\t10\t10\t2", "05\t10\t10\t3", "05\t20\t20\t4", "05\t20\t20\t5", "05\t15\t15\t6",
            "06\t20\t20\t1", "06\t15\t15\t2", "06\t10\t10\t3", "06\t20\t20\t4", "06\t20\t20\t5", "06\t20\t20\t6"
        )
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession),
            droptable = true,
            inTable = tableName,
            minsize = 2,
            minsup = 3,
            bin_s = 1,
            timeScale = AbsoluteScale,
            bin_t = 1,
            eps_t = 6,
            returnResult = true
        )
        val expectedClusters = Set(
            (RoaringBitmap.bitmapOf(0, 1), 2, 4),
            (RoaringBitmap.bitmapOf(1, 2), 2, 3),
            (RoaringBitmap.bitmapOf(2, 3), 2, 3),
            (RoaringBitmap.bitmapOf(3, 4), 2, 4),
            (RoaringBitmap.bitmapOf(4, 5), 2, 4)
        )
        assertEquals(expectedClusters.size, cuteClusters._1)
    }

    /** Test the recognition of a Convoy pattern. */
    @Test def testConvoyDetection(): Unit = {
        val test_name = "Convoy_DETECTION"
        val inputSet: Array[String] = Array( //
            "01\t00\t00\t1", "01\t00\t00\t2", "01\t00\t00\t3", "01\t00\t00\t4", "01\t00\t00\t5", "01\t00\t00\t6", //
            "02\t00\t00\t1", "02\t00\t00\t2", "02\t10\t10\t3", "02\t00\t00\t4", "02\t00\t00\t5", "02\t05\t05\t6", //
            "03\t10\t10\t1", "03\t10\t10\t2", "03\t10\t10\t3", "03\t00\t00\t4", "03\t05\t05\t5", "03\t05\t05\t6", //
            "04\t10\t10\t1", "04\t10\t10\t2", "04\t10\t10\t3", "04\t10\t10\t4", "04\t20\t20\t5", "04\t15\t15\t6", //
            "05\t20\t20\t1", "05\t10\t10\t2", "05\t10\t10\t3", "05\t20\t20\t4", "05\t20\t20\t5", "05\t15\t15\t6", //
            "06\t20\t20\t1", "06\t15\t15\t2", "06\t10\t10\t3", "06\t20\t20\t4", "06\t20\t20\t5", "06\t20\t20\t6"
        )
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession),
            droptable = true,
            inTable = tableName,
            minsize = 2,
            minsup = 3,
            bin_s = 1,
            timeScale = AbsoluteScale,
            bin_t = 1,
            eps_t = 1,
            returnResult = true
        )
        val expectedClusters = Set(
            (RoaringBitmap.bitmapOf(2, 3), 2, 3),
            (RoaringBitmap.bitmapOf(4, 5), 2, 3)
        )
        assertEquals(expectedClusters.size, cuteClusters._1)
    }

    /** Test the recognition of a Convoy pattern. */
    @Test def testConvoyDetectionFromPaper(): Unit = {
        val test_name = "testConvoyDetectionFromPaper"
        val inputSet: Array[String] = Array( //
            "01\t1\t0\t1", "01\t2\t0\t1", "01\t3\t0\t2", "01\t1\t0\t3", "01\t1\t0\t4", "01\t2\t0\t5", "01\t3\t0\t6", "01\t1\t0\t7", //
            "02\t1\t0\t1", "02\t2\t0\t1", "02\t3\t0\t2", "02\t1\t0\t3", "02\t2\t0\t4", "02\t3\t0\t5", "02\t3\t0\t6", "02\t1\t0\t7" //
        )
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession),
            droptable = true,
            inTable = tableName,
            minsize = 2,
            minsup = 4,
            bin_s = 1,
            timeScale = AbsoluteScale,
            bin_t = 1,
            eps_t = 1,
            returnResult = true
        )
        val expectedClusters = Set(
            (RoaringBitmap.bitmapOf(0, 1), 2, 6)
        )
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }

    /** Test the recognition of a Convoy pattern. */
    @Test def testConvoy(): Unit = {
        val test_name = "testConvoyDetectionFromPaper"
        val inputSet: Array[String] = Array( //
            "01\t0\t0\t1", "01\t0\t0\t1", "01\t3\t0\t2", "01\t0\t0\t3", "01\t0\t0\t4", "01\t0\t0\t5", "01\t0\t0\t6", "01\t0\t0\t7", //
            "02\t0\t0\t1", "02\t0\t0\t1", "02\t4\t0\t2", "02\t0\t0\t3", "02\t0\t0\t4", "02\t0\t0\t5", "02\t0\t0\t6", "02\t0\t0\t7", //
            "03\t0\t0\t1", "03\t1\t0\t1", "03\t0\t0\t2", "03\t1\t0\t3", "03\t0\t0\t4", "03\t1\t0\t5", "03\t0\t0\t6", "03\t1\t0\t7", //
            "04\t1\t0\t1", "04\t0\t0\t1", "04\t1\t0\t2", "04\t0\t0\t3", "04\t1\t0\t4", "04\t0\t0\t5", "04\t1\t0\t6", "04\t0\t0\t7" //
        )
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession),
            droptable = true,
            inTable = tableName,
            minsize = 2,
            minsup = 4,
            bin_s = 1,
            timeScale = AbsoluteScale,
            bin_t = 1,
            eps_t = 1,
            returnResult = true
        )
        val expectedClusters = Set(
            (RoaringBitmap.bitmapOf(0, 1), 2, 6)
        )
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }

    /** Test the recognition of a Convoy pattern. */
    @Test def testConvoyDetectionFromPaperFail(): Unit = {
        val test_name = "testConvoyDetectionFromPaperFail"
        val inputSet: Array[String] = Array( //
            "01\t1\t0\t1", "01\t3\t0\t2", "01\t1\t0\t3", "01\t1\t0\t4", "01\t2\t0\t5", "01\t3\t0\t6", "01\t1\t0\t7", //
            "02\t1\t0\t1", "02\t3\t0\t2", "02\t1\t0\t3", "02\t2\t0\t4", "02\t3\t0\t5", "02\t3\t0\t6", "02\t1\t0\t7" //
        )
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession),
            droptable = true,
            inTable = tableName,
            minsize = 2,
            minsup = 4,
            bin_s = 1,
            timeScale = AbsoluteScale,
            bin_t = 1,
            eps_t = 1,
            returnResult = true
        )
        val expectedClusters = Set()
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }

    /**
     * Test recognition of a Group pattern.
     */
    @Test def testGroupDetection(): Unit = {
        val test_name = "GROUP_DETECTION"
        val inputSet: Array[String] = Array( //
            "01\t00\t00\t1", "01\t00\t00\t2", "01\t00\t00\t3", "01\t00\t00\t4", "01\t00\t00\t5", "01\t00\t00\t6", //
            "02\t00\t00\t1", "02\t00\t00\t2", "02\t10\t10\t3", "02\t00\t00\t4", "02\t00\t00\t5", "02\t05\t05\t6", //
            "03\t10\t10\t1", "03\t10\t10\t2", "03\t10\t10\t3", "03\t00\t00\t4", "03\t05\t05\t5", "03\t05\t05\t6", //
            "04\t10\t10\t1", "04\t10\t10\t2", "04\t10\t10\t3", "04\t10\t10\t4", "04\t20\t20\t5", "04\t15\t15\t6", //
            "05\t20\t20\t1", "05\t10\t10\t2", "05\t10\t10\t3", "05\t20\t20\t4", "05\t20\t20\t5", "05\t15\t15\t6", //
            "06\t20\t20\t1", "06\t15\t15\t2", "06\t10\t10\t3", "06\t20\t20\t4", "06\t20\t20\t5", "06\t20\t20\t6"
        )
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession),
            droptable = true,
            inTable = tableName,
            minsize = 2,
            minsup = 3,
            bin_s = 1,
            timeScale = AbsoluteScale,
            bin_t = 1,
            eps_t = 2,
            returnResult = true
        )
        val expectedClusters = Set(
            (RoaringBitmap.bitmapOf(0, 1), 2, 4),
            (RoaringBitmap.bitmapOf(1, 2), 2, 3),
            (RoaringBitmap.bitmapOf(2, 3), 2, 3),
            (RoaringBitmap.bitmapOf(3, 4), 2, 3),
            (RoaringBitmap.bitmapOf(4, 5), 2, 4)
        )
        assertEquals(expectedClusters.size, cuteClusters._1)
    }

    @Test def testWeeklyContiguityData(): Unit = {
        val test_name = "WEEKLY_CONTIGUITY_DETECTION"
        val monday10AMStamp = 1578910464L
        val monday11AMStamp = 1578914064L
        val monday12AMStamp = 1578917664L
        val inputSet: Array[String] =
            Array(
                s"01\t0\t0\t$monday10AMStamp",
                s"01\t0\t0\t$monday11AMStamp",
                s"01\t0\t0\t$monday12AMStamp",
                s"02\t0\t0\t$monday10AMStamp",
                s"02\t0\t0\t$monday11AMStamp",
                s"02\t0\t0\t$monday12AMStamp")
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession),
            timeScale = DailyScale,
            droptable = true,
            inTable = tableName,
            minsize = 2,
            minsup = 2,
            bin_s = 1,
            bin_t = 1,
            eps_t = 1,
            returnResult = true
        )
        val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 3))
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }

    /**
     * Check that contiguity works also with relaxed time constrains
     */
    @Test def testWeeklySmootherContiguityClusters(): Unit = {
        val test_name = "WEEKLY_SMOOTHER_CONTIGUITY_CHECK"
        val monday10AMStamp = 1578910464L
        val monday11AMStamp = 1578914064L
        val monday13PMStamp = 1578921264L
        val monday14PMStamp = 1578924864L
        val inputSet: Array[String] = Array( //
            s"01\t0\t0\t$monday10AMStamp", s"01\t0\t0\t$monday11AMStamp", //
            s"01\t0\t0\t$monday13PMStamp", s"01\t0\t0\t$monday14PMStamp", //
            s"02\t0\t0\t$monday10AMStamp", s"02\t0\t0\t$monday11AMStamp", //
            s"02\t0\t0\t$monday13PMStamp", s"02\t0\t0\t$monday14PMStamp")
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession),
            droptable = true,
            timeScale = DailyScale,
            inTable = tableName,
            minsize = 2,
            minsup = 3,
            bin_s = 1,
            bin_t = 1,
            eps_t = 2,
            returnResult = true
        )
        val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 4))
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }

    /** Check Convoy patterns on Weekly based definition. */
    @Test def testWeeklyConvoyClusters(): Unit = {
        val test_name = "WEEKLY_Convoy_CHECK"
        val sunday10PMStamp = 1578868341L
        val sunday11PMStamp = 1578871941L
        val monday01AMStamp = 1578879141L
        val monday02AMStamp = 1578882741L
        val inputSet: Array[String] = Array( //
            s"01\t0\t0\t$sunday10PMStamp", s"01\t0\t0\t$sunday11PMStamp", //
            s"01\t0\t0\t$monday01AMStamp", s"01\t0\t0\t$monday02AMStamp", //
            s"02\t0\t0\t$sunday10PMStamp", s"02\t0\t0\t$sunday11PMStamp", //
            s"02\t0\t0\t$monday01AMStamp", s"02\t0\t0\t$monday02AMStamp")
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession), droptable = true, timeScale = DailyScale, inTable = tableName, minsize = 2, minsup = 2, bin_s = 1, bin_t = 1, eps_t = 1, returnResult = true)
        val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 4))
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }

    /** Check Convoy patterns on Weekly based definition. */
    @Test def testWeeklySwarmClusters(): Unit = {
        val test_name = "WEEKLY_SWARM_CHECK"
        val sunday10PMStamp = 1578868341L
        val sunday11PMStamp = 1578871941L
        val monday01AMStamp = 1578879141L
        val monday02AMStamp = 1578882741L
        val inputSet: Array[String] = Array( //
            s"01\t0\t0\t$sunday10PMStamp", //
            s"01\t0\t0\t$sunday11PMStamp", //
            s"01\t0\t0\t$monday01AMStamp", //
            s"01\t0\t0\t$monday02AMStamp", //
            s"02\t0\t0\t$sunday10PMStamp", //
            s"02\t0\t0\t$sunday11PMStamp", //
            s"02\t0\t0\t$monday01AMStamp", //
            s"02\t0\t0\t$monday02AMStamp")
        val tableName = s"tmp_$test_name"
        loadAndStoreDataset(inputSet, tableName, sparkSession)
        val cuteClusters = Main.run(spark = Some(sparkSession), droptable = true, timeScale = WeeklyScale, inTable = tableName, minsize = 2, minsup = 2, bin_s = 1, returnResult = true)
        val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 2))
        assertEquals(expectedClusters, cuteClusters._2.toSet)
    }
}