package it.unibo.big

import it.unibo.big.TemporalScale.{AbsoluteScale, DailyScale, NoScale, WeeklyScale}
import it.unibo.big.test.TempTableLoader
import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, assertTrue}
import org.roaringbitmap.RoaringBitmap
import org.scalatest._

class TestOnCluster extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  @transient var sparkSession: SparkSession = _

  type cuteCluster = (RoaringBitmap, Int, Int)
  val dataLoader: TempTableLoader = TempTableLoader()

  override def beforeAll(): Unit = {
    sparkSession = TestPaper.startSparkTestSession()
  }
  
  test("testDB") {
    var res9 = CTM.run(spark=Some(sparkSession), droptable = true, minsize = 1, minsup = 25, bin_s = 10, inTable = "trajectory.besttrj_standard", timeScale = NoScale, returnResult = true)
    assertEquals(2466, res9._2.length)
    res9 = CTM.run(spark=Some(sparkSession), droptable = false, minsize = 1, minsup = 25, bin_s = 10, inTable = "trajectory.besttrj_standard", timeScale = NoScale)
    assertEquals(2466, res9._1)
    assertTrue(2466 > res9._2.length)
  }

  /**
   * Test a pattern where the trajectories are in three near cells.
   */
  test("testAbsoluteContiguityClusters") {
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
    dataLoader.loadAndStoreDataset(absoluteContiguityInputSet, absoluteContiguityTableView, sparkSession)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
      droptable = true,
      inTable = absoluteContiguityTableView,
      minsize = 2,
      minsup = 2,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      eps_t = 1,
      returnResult = true
    )
    val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 3))
    assertEquals(expectedClusters, cuteClusters._2.toSet)
  }

  /**
   * Test a pattern where the trajectories are in three contigued cells.
   */
  test("testAbsoluteContiguityClustersNOResult") {
    // println("---- absolute contiguity no result test 1 temporal bucket size")
    val test_name = "AbsoluteContiguityClustersNOResult"
    val absoluteContiguityInputSet: Array[String] = Array("01\t0\t0\t1", "01\t0\t0\t2", "01\t0\t0\t3", "02\t0\t0\t1", "02\t0\t0\t2", "02\t0\t0\t4")
    val absoluteContiguityTableView = "simple_table_view_NOresult"
    dataLoader.loadAndStoreDataset(absoluteContiguityInputSet, absoluteContiguityTableView, sparkSession)
    val absoluteContiguityClusters = CTM.run(
      spark=Some(sparkSession),
      droptable = true,
      inTable = absoluteContiguityTableView,
      minsize = 2,
      minsup = 3,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      eps_t = 1,
      returnResult = true
    )
    assertTrue(absoluteContiguityClusters._2.isEmpty)
  }

  /**
   * Check that contiguity works also with relaxed time constrains
   */
  test("testSmootherContiguityClusters") {
    val test_name = "SMOOTHER_CONTIGUITY_CHECK"
    // println(s"----$test_name----")
    val inputSet: Array[String] = Array( //
      "01\t0\t0\t1","01\t0\t0\t2", "01\t0\t0\t4", "01\t0\t0\t6", //
      "02\t0\t0\t1", "02\t0\t0\t2", "02\t0\t0\t4", "02\t0\t0\t6")

    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)

    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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
    val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 4))
    assertEquals(expectedClusters, cuteClusters._2.toSet)
  }

  /** Check that contiguity works also with relaxed time constrains. */
  test("testSmootherContiguityTwoThresholdClusters") {
    val test_name = "SMOOTHER_CONTIGUITY_TWO_CHECK"
    // println(s"----$test_name----")
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
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
      droptable = true,
      inTable = tableName,
      minsize = 2,
      minsup = 2,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      eps_t = 2,
      returnResult = true
    )
    val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 4))
    assertEquals(expectedClusters, cuteClusters._2.toSet)
  }

  /**
   * Check that contiguity works also with relaxed time costrains, no result should be found here.
   */
  test("testSmootherContiguityClustersNOResult") {
    val test_name = "SMOOTHER_CONTIGUITY_CHECK_NO_RESULT"
    // println(s"----$test_name----")
    val inputSet: Array[String] = Array( //
      "01\t0\t0\t1", "01\t0\t0\t2", "01\t0\t0\t5", "01\t0\t0\t7", //
      "02\t0\t0\t1", "02\t0\t0\t2", "02\t0\t0\t5", "02\t0\t0\t7")
    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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

  /**
   * This test include a cell with an superior ID that contains the pattern required but is excluded for some
   * spatio-temporal reason.
   */

  test("testExternalNeighbourClusters") {
    val test_name = "EXTERNAL_NEIGHBOUR_CHECK"
    // println(s"----$test_name----")
    val inputSet: Array[String] = Array( //
      "01\t10\t10\t1", "01\t0\t0\t3", "01\t0\t0\t4", "01\t0\t0\t5", //
      "02\t10\t10\t1", "02\t0\t0\t3", "02\t0\t0\t4", "02\t0\t0\t5")
    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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
  test("testExternalNeighbourInsideTheIDPathClusters") {
    val test_name = "EXTERNAL_PATH_NEIGHBOUR_CHECK"
    // println(s"----$test_name----")
    val inputSet: Array[String] = Array( //
      "01\t10\t10\t1", "01\t0\t0\t3", "01\t10\t10\t4", "01\t10\t10\t5", //
      "02\t10\t10\t1", "02\t0\t0\t3", "02\t10\t10\t4", "02\t10\t10\t5")
    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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
  test("testTwoSplitsClusters") {
    val test_name = "TWO_SPLITS_CHECK"
    // println(s"----$test_name----")

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
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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

  test("testSwarmDetection") {
    val test_name = "SWARM_DETECTION"
    // println(s"----$test_name----")

    val inputSet: Array[String] = Array(
      "01\t00\t00\t1", "01\t00\t00\t2", "01\t00\t00\t3", "01\t00\t00\t4", "01\t00\t00\t5", "01\t00\t00\t6",
      "02\t00\t00\t1", "02\t00\t00\t2", "02\t10\t10\t3", "02\t00\t00\t4", "02\t00\t00\t5", "02\t05\t05\t6",
      "03\t10\t10\t1", "03\t10\t10\t2", "03\t10\t10\t3", "03\t00\t00\t4", "03\t05\t05\t5", "03\t05\t05\t6",
      "04\t10\t10\t1", "04\t10\t10\t2", "04\t10\t10\t3", "04\t10\t10\t4", "04\t20\t20\t5", "04\t15\t15\t6",
      "05\t20\t20\t1", "05\t10\t10\t2", "05\t10\t10\t3", "05\t20\t20\t4", "05\t20\t20\t5", "05\t15\t15\t6",
      "06\t20\t20\t1", "06\t15\t15\t2", "06\t10\t10\t3", "06\t20\t20\t4", "06\t20\t20\t5", "06\t20\t20\t6"
    )
    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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
    // assertEquals(expectedClusters, cuteClusters._2.toSet)
    assertEquals(expectedClusters.size, cuteClusters._1)
  }

  /**  Test the recognition of a flock pattern. */

  test("testFlockDetection") {
    val test_name = "FLOCK_DETECTION"
    // println(s"----$test_name----")
    val inputSet: Array[String] = Array( //
      "01\t00\t00\t1", "01\t00\t00\t2", "01\t00\t00\t3", "01\t00\t00\t4", "01\t00\t00\t5", "01\t00\t00\t6", //
      "02\t00\t00\t1", "02\t00\t00\t2", "02\t10\t10\t3", "02\t00\t00\t4", "02\t00\t00\t5", "02\t05\t05\t6", //
      "03\t10\t10\t1", "03\t10\t10\t2", "03\t10\t10\t3", "03\t00\t00\t4", "03\t05\t05\t5", "03\t05\t05\t6", //
      "04\t10\t10\t1", "04\t10\t10\t2", "04\t10\t10\t3", "04\t10\t10\t4", "04\t20\t20\t5", "04\t15\t15\t6", //
      "05\t20\t20\t1", "05\t10\t10\t2", "05\t10\t10\t3", "05\t20\t20\t4", "05\t20\t20\t5", "05\t15\t15\t6", //
      "06\t20\t20\t1", "06\t15\t15\t2", "06\t10\t10\t3", "06\t20\t20\t4", "06\t20\t20\t5", "06\t20\t20\t6"
    )
    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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
    // assertEquals(expectedClusters, cuteClusters._2.toSet)
    assertEquals(expectedClusters.size, cuteClusters._1)
  }

  /**  Test the recognition of a flock pattern. */
  test("testFlockDetectionFromPaper") {
    val test_name = "testFlockDetectionFromPaper"
    // println(s"----$test_name----")
    val inputSet: Array[String] = Array( //
      "01\t1\t0\t1", "01\t2\t0\t1", "01\t3\t0\t2", "01\t1\t0\t3", "01\t1\t0\t4", "01\t2\t0\t5", "01\t3\t0\t6", "01\t1\t0\t7", //
      "02\t1\t0\t1", "02\t2\t0\t1", "02\t3\t0\t2", "02\t1\t0\t3", "02\t2\t0\t4", "02\t3\t0\t5", "02\t3\t0\t6", "02\t1\t0\t7" //
    )
    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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

  /**  Test the recognition of a flock pattern. */
    
  test("FlockDetectionFromPaperFail") {
    val test_name = "testFlockDetectionFromPaperFail"
    // println(s"----$test_name----")
    val inputSet: Array[String] = Array( //
      "01\t1\t0\t1", "01\t3\t0\t2", "01\t1\t0\t3", "01\t1\t0\t4", "01\t2\t0\t5", "01\t3\t0\t6", "01\t1\t0\t7", //
      "02\t1\t0\t1", "02\t3\t0\t2", "02\t1\t0\t3", "02\t2\t0\t4", "02\t3\t0\t5", "02\t3\t0\t6", "02\t1\t0\t7" //
    )
    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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
  test("GroupDetection") {
    val test_name = "GROUP_DETECTION"
    // println(s"----$test_name----")

    val inputSet: Array[String] = Array( //
      "01\t00\t00\t1", "01\t00\t00\t2", "01\t00\t00\t3", "01\t00\t00\t4", "01\t00\t00\t5", "01\t00\t00\t6", //
      "02\t00\t00\t1", "02\t00\t00\t2", "02\t10\t10\t3", "02\t00\t00\t4", "02\t00\t00\t5", "02\t05\t05\t6", //
      "03\t10\t10\t1", "03\t10\t10\t2", "03\t10\t10\t3", "03\t00\t00\t4", "03\t05\t05\t5", "03\t05\t05\t6", //
      "04\t10\t10\t1", "04\t10\t10\t2", "04\t10\t10\t3", "04\t10\t10\t4", "04\t20\t20\t5", "04\t15\t15\t6", //
      "05\t20\t20\t1", "05\t10\t10\t2", "05\t10\t10\t3", "05\t20\t20\t4", "05\t20\t20\t5", "05\t15\t15\t6", //
      "06\t20\t20\t1", "06\t15\t15\t2", "06\t10\t10\t3", "06\t20\t20\t4", "06\t20\t20\t5", "06\t20\t20\t6"
    )
    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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
    // assertEquals(expectedClusters, cuteClusters._2.toSet)
    assertEquals(expectedClusters.size, cuteClusters._1)
  }

  test("WeeklyContiguityData") {
    val test_name = "WEEKLY_CONTIGUITY_DETECTION"
    // println(s"----$test_name----")
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
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession, 1)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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
  test("WeeklySmootherContiguityClusters") {
    val test_name = "WEEKLY_SMOOTHER_CONTIGUITY_CHECK"
    // println(s"----$test_name----")
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
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession, 1)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
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

  /** Check Flock patterns on Weekly based definition. */
  test("WeeklyFlockClusters") {
    val test_name = "WEEKLY_FLOCK_CHECK"
    // println(s"----$test_name----")
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
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession, 1)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
      droptable = true,
      timeScale = DailyScale,
      inTable = tableName,
      minsize = 2,
      minsup = 2,
      bin_s = 1,
      bin_t = 1,
      eps_t = 1,
      returnResult = true
    )
    val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 4))
    assertEquals(expectedClusters, cuteClusters._2.toSet)
  }

  /** Check Flock patterns on Weekly based definition. */
  test("WeeklySwarmClusters") {
    val test_name = "WEEKLY_SWARM_CHECK"
    // println(s"----$test_name----")
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
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession, 1)
    val cuteClusters = CTM.run(spark=Some(sparkSession), 
      droptable = true,
      timeScale = WeeklyScale,
      inTable = tableName,
      minsize = 2,
      minsup = 2,
      bin_s = 1,
      returnResult = true
    )
    val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 2))
    assertEquals(expectedClusters, cuteClusters._2.toSet)
  }
}