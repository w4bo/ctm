package it.unibo.big.test

import it.unibo.big.TemporalScale.{AbsoluteScale, DailyScale, NoScale, WeeklyScale}
import it.unibo.big.{CTM, Utils}
import org.apache.spark.sql.SparkSession
import org.roaringbitmap.RoaringBitmap

object TestOnCluster {

  @transient val sparkSession: SparkSession = Utils.startSparkSession()

  /**
   * Main of the whole application.
   *
   * @param args arguments
   */
  def main(args: Array[String]): Unit = {
    val dropTableFlag = true
    this.oldcuteTest()
    this.testAbsoluteContiguityClusters(dropTableFlag)
    this.testAbsoluteContiguityClustersNOResult(dropTableFlag)
    this.testSmootherContiguityClusters(dropTableFlag)
    this.testAbsoluteContiguityClustersNOResult(dropTableFlag)
    this.testExternalNeighbourClusters(dropTableFlag)
    this.testExternalNeighbourInsideTheIDPathClusters(dropTableFlag)
    this.testSmootherContiguityTwoThresholdClusters(dropTableFlag)
    this.testTwoSplitsClusters(dropTableFlag)
    this.testSwarmDetection(dropTableFlag)
    this.testFlockDetection(dropTableFlag)
    this.testGroupDetection(dropTableFlag)
    this.testWeeklyContiguityData(dropTableFlag)
    this.testWeeklySmootherContiguityClusters(dropTableFlag)
    this.testWeeklyFlockClusters(dropTableFlag)
    this.testWeeklySwarmClusters(dropTableFlag)
  }

  /** Alias for a cluster. */
  type cuteCluster = (RoaringBitmap, Int, Int)
  val dataLoader = TempTableLoader()

  def oldcuteTest(): Unit = {
    var res9 = CTM.run(droptable = true, minsize = 1, minsup = 25, bin_s = 10, inTable = "trajectory.besttrj_standard", timeScale = NoScale, returnResult = true)
    require(res9._2.length == 2466, s"Test 9.e failed. Expected: ${2466}, got: ${res9._1}")
    res9 = CTM.run(droptable = false, minsize = 1, minsup = 25, bin_s = 10, inTable = "trajectory.besttrj_standard", timeScale = NoScale)
    require(res9._1 == 2466, s"Test 9.e failed. Expected: ${2466}, got: ${res9._1}")
    require(res9._2.length < 2466, s"Test 9.e failed. Expected: < ${2466}, got: ${res9._1}") // this is because we store the itemsets in while generating them
  }

  /**
   * Test a pattern where the trajectories are in three near cells.
   */
  def testAbsoluteContiguityClusters(dropTableFlag: Boolean): Unit = {
    val test_name = "AbsoluteContiguityClusters"
    val absoluteContiguityInputSet: Array[String] =
      Array(
        "01\t0\t0\t1",
        "01\t0\t0\t2",
        "01\t0\t0\t3",
        "02\t0\t0\t1",
        "02\t0\t0\t2",
        "02\t0\t0\t3")
    val absoluteContiguityTableView = "simple_table_view"
    dataLoader.loadAndStoreDataset(absoluteContiguityInputSet, absoluteContiguityTableView, sparkSession)
    val cuteClusters = CTM.run(
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
    require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
  }

  /**
   * Test a pattern where the trajectories are in three contigued cells.
   */
  def testAbsoluteContiguityClustersNOResult(dropTableFlag: Boolean): Unit = {
    println("---- absolute contiguity no result test 1 temporal bucket size")
    val test_name = "AbsoluteContiguityClustersNOResult"
    val absoluteContiguityInputSet: Array[String] = Array("01\t0\t0\t1", "01\t0\t0\t2", "01\t0\t0\t3", "02\t0\t0\t1", "02\t0\t0\t2", "02\t0\t0\t4")
    val absoluteContiguityTableView = "simple_table_view_NOresult"
    dataLoader.loadAndStoreDataset(absoluteContiguityInputSet, absoluteContiguityTableView, sparkSession)
    val absoluteContiguityClusters = CTM.run(
      droptable = dropTableFlag,
      inTable = absoluteContiguityTableView,
      minsize = 2,
      minsup = 3,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      eps_t = 1,
      returnResult = true
    )
    require(absoluteContiguityClusters._2.isEmpty, s"$test_name expected empty, got ${absoluteContiguityClusters._2.toSet}")
  }

  /**
   * Check that contiguity works also with relaxed time constrains
   */
  def testSmootherContiguityClusters(dropTableFlag: Boolean): Unit = {
    val test_name = "SMOOTHER_CONTIGUITY_CHECK"
    println(s"----$test_name----")
    val inputSet: Array[String] = Array( //
      "01\t0\t0\t1", "01\t0\t0\t2", "01\t0\t0\t4", "01\t0\t0\t6", //
      "02\t0\t0\t1", "02\t0\t0\t2", "02\t0\t0\t4", "02\t0\t0\t6")

    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)

    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
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
    require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
  }

  /** Check that contiguity works also with relaxed time constrains. */
  def testSmootherContiguityTwoThresholdClusters(dropTableFlag: Boolean): Unit = {
    val test_name = "SMOOTHER_CONTIGUITY_TWO_CHECK"
    println(s"----$test_name----")
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
    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
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
    require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
  }

  /**
   * Check that contiguity works also with relaxed time costrains, no result should be found here.
   */
  def testSmootherContiguityClustersNOResult(dropTableFlag: Boolean): Unit = {
    val test_name = "SMOOTHER_CONTIGUITY_CHECK_NO_RESULT"
    println(s"----$test_name----")
    val inputSet: Array[String] = Array( //
      "01\t0\t0\t1", "01\t0\t0\t2", "01\t0\t0\t5", "01\t0\t0\t7", //
      "02\t0\t0\t1", "02\t0\t0\t2", "02\t0\t0\t5", "02\t0\t0\t7")
    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
      inTable = tableName,
      minsize = 2,
      minsup = 3,
      bin_s = 1,
      timeScale = AbsoluteScale,
      bin_t = 1,
      eps_t = 2,
      returnResult = true
    )
    require(cuteClusters._2.isEmpty, s"$test_name: expected to be empty; found ${cuteClusters._1}")
  }

  /**
   * This test include a cell with an superior ID that contains the pattern required but is excluded for some
   * spatio-temporal reason.
   */
  def testExternalNeighbourClusters(dropTableFlag: Boolean): Unit = {
    val test_name = "EXTERNAL_NEIGHBOUR_CHECK"
    println(s"----$test_name----")
    val inputSet: Array[String] = Array( //
      "01\t10\t10\t1", "01\t0\t0\t3", "01\t0\t0\t4", "01\t0\t0\t5", //
      "02\t10\t10\t1", "02\t0\t0\t3", "02\t0\t0\t4", "02\t0\t0\t5")
    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
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
    require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
  }

  /**
   * This test include a cell with an superior ID that contains the pattern required but is excluded for some
   * spatio-temporal reason.
   */
  def testExternalNeighbourInsideTheIDPathClusters(dropTableFlag: Boolean): Unit = {
    val test_name = "EXTERNAL_PATH_NEIGHBOUR_CHECK"
    println(s"----$test_name----")
    val inputSet: Array[String] = Array( //
      "01\t10\t10\t1", "01\t0\t0\t3", "01\t10\t10\t4", "01\t10\t10\t5", //
      "02\t10\t10\t1", "02\t0\t0\t3", "02\t10\t10\t4", "02\t10\t10\t5")
    val tableName = s"tmp_$test_name"
    dataLoader.loadAndStoreDataset(inputSet, tableName, sparkSession)
    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
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
    require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
  }

  /**
   * This test include a cell with an superior ID that contains the pattern required but is excluded for some
   * spatio-temporal reason.
   */
  def testTwoSplitsClusters(dropTableFlag: Boolean): Unit = {
    val test_name = "TWO_SPLITS_CHECK"
    println(s"----$test_name----")

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
    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
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
    require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
    println(s"----$test_name: PASSED------")
  }

  /**
   * Test the recognition of a Swarm pattern.
   */
  def testSwarmDetection(dropTableFlag: Boolean): Unit = {
    val test_name = "SWARM_DETECTION"
    println(s"----$test_name----")

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
    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
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
    // require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
    require(cuteClusters._1 == expectedClusters.size, s"$test_name: expected \n${expectedClusters.size} clusters got \n${cuteClusters._2.toSet}")
  }

  /**  Test the recognition of a flock pattern. */
  def testFlockDetection(dropTableFlag: Boolean): Unit = {
    val test_name = "FLOCK_DETECTION"
    println(s"----$test_name----")
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
    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
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
    // require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
    require(cuteClusters._1 == expectedClusters.size, s"$test_name: expected \n${expectedClusters.size} clusters got \n${cuteClusters._2.toSet}")
  }

  /**
   * Test recognition of a Group pattern.
   */
  def testGroupDetection(dropTableFlag: Boolean): Unit = {
    val test_name = "GROUP_DETECTION"
    println(s"----$test_name----")

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
    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
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
    // require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
    require(cuteClusters._1 == expectedClusters.size, s"$test_name: expected \n${expectedClusters.size} clusters got \n${cuteClusters._2.toSet}")
  }

  def testWeeklyContiguityData(dropTableFlag: Boolean): Unit = {
    val test_name = "WEEKLY_CONTIGUITY_DETECTION"
    println(s"----$test_name----")
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
    val cuteClusters = CTM.run(
      timeScale = DailyScale,
      droptable = dropTableFlag,
      inTable = tableName,
      minsize = 2,
      minsup = 2,
      bin_s = 1,
      bin_t = 1,
      eps_t = 1,
      returnResult = true
    )
    val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 3))
    require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
  }

  /**
   * Check that contiguity works also with relaxed time constrains
   */
  def testWeeklySmootherContiguityClusters(dropTableFlag: Boolean): Unit = {
    val test_name = "WEEKLY_SMOOTHER_CONTIGUITY_CHECK"
    println(s"----$test_name----")
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
    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
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
    require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
  }

  /** Check Flock patterns on Weekly based definition. */
  def testWeeklyFlockClusters(dropTableFlag: Boolean): Unit = {
    val test_name = "WEEKLY_FLOCK_CHECK"
    println(s"----$test_name----")
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
    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
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
    require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
  }

  /** Check Flock patterns on Weekly based definition. */
  def testWeeklySwarmClusters(dropTableFlag: Boolean): Unit = {
    val test_name = "WEEKLY_SWARM_CHECK"
    println(s"----$test_name----")
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
    val cuteClusters = CTM.run(
      droptable = dropTableFlag,
      timeScale = WeeklyScale,
      inTable = tableName,
      minsize = 2,
      minsup = 2,
      bin_s = 1,
      returnResult = true
    )
    val expectedClusters = Set((RoaringBitmap.bitmapOf(0, 1), 2, 2))
    require(cuteClusters._2.toSet.equals(expectedClusters), s"$test_name: expected\n${expectedClusters}\ngot:\n${cuteClusters._2.toSet}")
  }
}
