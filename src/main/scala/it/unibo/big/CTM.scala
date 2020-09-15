package it.unibo.big

import java.text.SimpleDateFormat
import java.util.Calendar

import it.unibo.big.TemporalScale.NoScale
import it.unibo.big.Utils._
import it.unibo.big.temporal.TemporalCellBuilder
import it.unibo.big.temporal.TemporalCellBuilder._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.roaringbitmap.RoaringBitmap
import org.rogach.scallop.ScallopConf

/** This is the main class of the project */
object CTM {

  def CTM(spark: SparkSession, trans: RDD[(Tid, Itemid)], brdNeighborhood: Option[Broadcast[Map[Tid, RoaringBitmap]]], minsup: Int, minsize: Int): (Long, Array[(RoaringBitmap, Tid, Tid)]) = {
    val spark.sparkContext = spark.sparkContext
    import spark.implicits._
    if (trans.count() == 0) {
      println("Empty transactions")
      return (0, Array())
    }

    /* *****************************************************************************************************************
     * BROADCASTING TRAJECTORIES IN CELL. This is the TT' used as transposed table as defined by Carpenter algorithm.
     * Also use RoaringbitMap for performance reason
     * ****************************************************************************************************************/
    print("--- Broadcasting brdTrajInCell (i.e., itemInTid)... ")
    val brdTrajInCell: Broadcast[Map[Tid, RoaringBitmap]] = spark.sparkContext.broadcast(
      trans
        .mapValues({ itemid: Itemid => RoaringBitmap.bitmapOf(itemid) })
        .reduceByKey((anItem, anotherItem) => {
          val m = RoaringBitmap.or(anItem, anotherItem)
          m.runOptimize()
          m
        }) // group by all the trajectories in the same cell
        .map(i => Array(i._1 -> i._2))
        .treeReduce(_ ++ _)
        .toMap
    )
    val nTransactions: Long = brdTrajInCell.value.size
    val brdTrajInCell_bytes = brdTrajInCell.value.values.map(_.getSizeInBytes + 4).sum
    println(brdTrajInCell_bytes + "B")
    if (debug || returnResult) {
      brdTrajInCell.value.foreach(println)
    }
    /* *****************************************************************************************************************
     * END - BROADCASTING TRAJECTORIES IN CELL
     * ****************************************************************************************************************/

    /**
     * Given a roaringbitmap of cell IDs compute all their neighbours as a RoaringBitMap.
     * @param cellSet a RoaringBitMap containing all the cellIDs.
     * @return a RoaringBitMap containing all the neighbours of all the cells.
     */
    def allNeighbours(cellSet: RoaringBitmap): RoaringBitmap = {
      val ret = RoaringBitmap.bitmapOf()
      cellSet.forEach(toJavaConsumer((cell: Integer) => {
        val elem = brdNeighborhood.get.value.get(cell)
        if (elem.isDefined) {
          ret.or(elem.get)
        }
      }))
      ret
    }

    /**
     * Given a set of CellIDs X and the support for the pattern common to all those cells, filter all the cells that does
     * not contain x as neighbour.
     * @param allCells the complete support of the itemset contained by all the cell in X.
     * @param x        a set of ID cells.
     * @return allCells filtered by removing all the cells that has id < minID(X) and that does not contain X in their neighbouhood.
     */
    def filterPreviousExistingCell(allCells: RoaringBitmap, x: RoaringBitmap): RoaringBitmap = {
      val minCell = x.toArray.min
      val ret: RoaringBitmap = RoaringBitmap.bitmapOf()
      allCells.forEach(toJavaConsumer((cell: Integer) => {
        val elem = brdNeighborhood.get.value.get(cell)
        if (cell >= minCell || elem.isDefined && elem.get.contains(minCell)) {
          ret.add(cell)
        }
      }))
      ret
    }
    /* *****************************************************************************************************************
     * END - BROADCASTING NEIGHBORHOOD
     * ****************************************************************************************************************/

    /**
     * Given a set of trajectoriesID, create a bitmap containing all the CellIDs that appear in at least one trajectory.
     * Given an itemset, this is used to find all the next cells to explore (i.e., to define R). This is dual to the
     * support, where we are looking for all the SHARED transactions.
     *
     * @param itemset a set of trajectory IDs as a bitmap.
     * @return a new RoaringBitmap containing the union of all the cellIDs.
     */
    def allCellsInTrajectories(itemset: RoaringBitmap): RoaringBitmap = {
      RoaringBitmap.bitmapOf(brdTrajInCell.value.filter({ case (_, transaction) => RoaringBitmap.intersects(itemset, transaction) }).keys.toSeq: _*)
    }

    /**
     * Given a set of trajectoryIDs, create a bitmap containing all the common cells between all the Trajectories.
     *
     * @param itemset a set of trajectoryIDs.
     * @return a Bitmap containing all the cells common to all the trajectories specified by the input.
     */
    def support(itemset: RoaringBitmap): RoaringBitmap = {
      RoaringBitmap.bitmapOf(brdTrajInCell.value.filter({ case (_, transaction) =>
        val iterator = itemset.iterator()
        var isOk = true
        while (iterator.hasNext && isOk) {
          isOk = transaction.contains(iterator.next())
        }
        isOk
      }).keys.toSeq: _*)
    }

    /**
     * Whether an itemset should be persisted. An itemset must be persisted when its support is above threshold
     * and its current support equals to the real support (i.e., the itemset has been completely explored).
     *
     * @param trajectorySet current cluster.
     * @return false if the itemset has been fully explored (i.e., it should not be extended further).
     */
    def toExtend(trajectorySet: RoaringBitmap, support: RoaringBitmap): Boolean = {
      !(support.getCardinality >= minsup)
    }

    /**
     * Keep only the cluster having a good size and support.
     *
     * @param trajectorySet the current trajectory set considered.
     * @param x             a set containing the current support of the trajectory set.
     * @param r             a set containing all possible cellset to be added to x.
     * @return true if the trajectory set is on its maximal path and has good size and support, false otherwise.
     */
    def filterCluster(trajectorySet: RoaringBitmap, x: RoaringBitmap, r: RoaringBitmap): Boolean = {
      var sup = support(trajectorySet)
      if (trajectorySet.getCardinality >= minsize && (x.getCardinality + r.getCardinality) >= minsup) {
        if (brdNeighborhood.isDefined) {
          sup = filterPreviousExistingCell(sup, x)
        }
        (if (brdNeighborhood.isEmpty) {
          x.contains(sup.getIntIterator.next())
        } else {
          true
        }) && RoaringBitmap.andNot(RoaringBitmap.andNot(sup, r), x).isEmpty
      } else {
        false
      }
    }

    /* *****************************************************************************************************************
     * Setting up the first cluster definition.
     * ****************************************************************************************************************/
    var curIteration: Int = 0
    val countOk = spark.sparkContext.longAccumulator
    val countToExtend = spark.sparkContext.longAccumulator
    var countStored: Long = 0
    var nItemsets: Long = 0
    var c: Long = 0

    // var startTime: Long = System.currentTimeMillis()
    CustomTimer.start()
    var clusters: RDD[CarpenterRowSet] =
      trans
        .mapValues({ trajId: Itemid => RoaringBitmap.bitmapOf(trajId) }) // This map the CellID -> TrajID converting the TrajID into a RoraingBitMap
        .reduceByKey((aTrajectory, bTrajectory) => RoaringBitmap.or(aTrajectory, bTrajectory)) // group by all the trajectories in the same cell
        .flatMap({ case (key: Tid, lCluster: RoaringBitmap) => // Now this is a Real TT' as intended by John Carpenter itself
          val X: RoaringBitmap = RoaringBitmap.bitmapOf(key) // current support
          val R: RoaringBitmap = RoaringBitmap.bitmapOf(brdTrajInCell.value.filter({ case (otherKey: Tid, _: RoaringBitmap) => key < otherKey }).toArray.map(_._1.toInt): _*) // select only the new cells
          // lCluster.runOptimize() // run RLE for performance reasons
          /* This is in the form of: RBM(trajID)  | isToExpand  | X as RBM which contains for now only the cell ID | R(bitMap of the following cells) as an RBM*/
          Array((lCluster, true, X, R))
        })
        .localCheckpoint()
    val clusterCount = clusters.count()
    println(s"\n--- Init clusters: $clusterCount")
    // var totalElapsedTime: Long = System.currentTimeMillis - startTime
    countOk.reset()
    countToExtend.reset()
    printCluster(clusters)
    /* *****************************************************************************************************************
     * END - Creating the transactional dataset
     * ****************************************************************************************************************/

    /**
     * Print a clusters RDD to STDOUT
     *
     * @param clusters the cluster RDD to print
     */
    def printCluster(clusters: RDD[CarpenterRowSet]): Unit = if (debug || returnResult) clusters.sortBy(i => -i._4.getCardinality).map(i => (i._1.toArray.toVector, i._2, i._3, i._4, support(i._1))).take(linesToPrint).foreach(println)

    do {
      curIteration += 1
      println(s"\n--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} $conf Iteration: " + curIteration)

      countOk.reset()
      countToExtend.reset()

      clusters =
        (if (curIteration % repfreq == 0) {
          clusters.repartition(partitions)
        } else {
          clusters
        }) // Shuffle the data every few iterations
          .flatMap({ case (lCluster: RoaringBitmap, extend: Boolean, x: RoaringBitmap, r: RoaringBitmap) =>
            // require(!RoaringBitmap.intersects(x, r), s"$x intersects $r")
            if (extend) { // If the cluster should be extended...
              val lClusterSupport = support(lCluster)
              var Y: RoaringBitmap = RoaringBitmap.and(lClusterSupport, r) // TIDs in all the rows
              var allXNeighbour = x
              if (brdNeighborhood.isDefined) {
                allXNeighbour = allNeighbours(x)
                Y = RoaringBitmap.and(Y, allXNeighbour)
              }
              val XplusY: RoaringBitmap = RoaringBitmap.or(x, Y) // X U Y = x + Y
              var R: RoaringBitmap = RoaringBitmap.and(RoaringBitmap.andNot(r, Y), allCellsInTrajectories(lCluster)) // rows to consider
              val toCheck: Array[(Tid, RoaringBitmap)] = brdTrajInCell.value.filter(t => R.contains(t._1)).toArray
              var t = (lCluster, toExtend(lCluster, lClusterSupport), XplusY, R)

              var expandedR = R
              if (brdNeighborhood.isDefined) {
                val allYNeighbour = allNeighbours(Y)
                val allXplusYNeighbour = RoaringBitmap.or(allXNeighbour, allYNeighbour)
                expandedR = RoaringBitmap.andNot(RoaringBitmap.and(R, allXplusYNeighbour), XplusY)
                // IF THERE IS A CELL ON WHICH THE PATTERN CAN BE EXPANDED; THEN THIS TUPLE CAN BE PRUNED
                if (!RoaringBitmap.and(expandedR, lClusterSupport).isEmpty) {
                  t = (lCluster, false, RoaringBitmap.bitmapOf(), RoaringBitmap.bitmapOf())
                }
              }
              var res: Iterable[CarpenterRowSet] =
              /* Cluster t should be expanded */
              // (if (!t._2 && disabledPruningForComparison || t._2) { // Should I compute the neighbors?
                toCheck
                  .filter({ case (key: Tid, _: RoaringBitmap) => expandedR.contains(key) })
                  .map({ case (key: Tid, rTransaction: RoaringBitmap) =>
                    val c = RoaringBitmap.and(lCluster, rTransaction) // C stores the common trajectories between the tuple Key and the lcluster
                    // c.runOptimize() // RUN LENGTH ENCODING TO SAVE MEMORY
                    R = RoaringBitmap.remove(R, key, key + 1) // Remove the key element from previous defined R
                    val incrementedBitMap = RoaringBitmap.add(XplusY, key, key + 1)
                    /*c becomes new lcluster, Add the element key to X*/
                    (c, true, incrementedBitMap, R)
                  }) ++ (if (t._2) Array[CarpenterRowSet]() else Array(t)) // Se sono da estendere non mi rimettere in gioco, altrimenti salvami
              res = res
                .filter(!_._3.isEmpty)
                .filter({ case (c: RoaringBitmap, _: Boolean, x: RoaringBitmap, r: RoaringBitmap) => filterCluster(c, x, r) })
              // the current element has already been counted, if it has to be saved don't count it twice
              // do not also count the elements that will be checked in the next phase (i.e., for which filter returns true)
              res.foreach(t => if (t._2) {
                countToExtend.add(1)
              } else {
                if (t._3.getCardinality >= minsup) countOk.add(1)
              })
              res
            } else {
              Array((lCluster, extend, x, r))
            }
          })
          .persist(StorageLevel.MEMORY_AND_DISK_SER) // .localCheckpoint()

      // Update clusters count values
      c = clusters.count()
      nItemsets += countOk.value
      println(s"--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} Done: ${c + countStored} (ok: $nItemsets [stored: $countStored], toExtend: ${countToExtend.value})")

      /* ***************************************************************************************************************
       * Iteratively store the itemsets to free memory (if necessary)
       * **************************************************************************************************************/
      val toAddIdx = spark.sparkContext.broadcast(countStored)
      if (!returnResult && (countToExtend.value == 0 || nItemsets - countStored >= storage_thr)) {
        if (storage_thr > 0) {
          println(s"--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} Writing itemsets to the database")
          clusters
            .filter({ case (_: RoaringBitmap, extend: Boolean, x: RoaringBitmap, _: RoaringBitmap) => !extend && x.getCardinality >= minsup })
            .map(cluster => (cluster._1, cluster._3))
            .zipWithIndex // .zipWithUniqueId
            .flatMap({ case (itemset: (RoaringBitmap, RoaringBitmap), uid: Long) =>
              val toAdd = toAddIdx.value
              val itemsetSize = itemset._1.getCardinality
              val itemsetSupp = itemset._2.getCardinality
              itemset._1.toArray.map(i => {
                require(i >= 0, "itemid is below zero")
                (uid + toAdd, i, itemsetSize, itemsetSupp) /*, icoh */
                // val icoh = coh(itemset._1)
              })
            }).toDF("itemsetid", "itemid", "size", "support").write.mode(if (countStored == 0) SaveMode.Overwrite else SaveMode.Append).saveAsTable(outTable)
          println(s"--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} Done writing")
        }
        countStored += nItemsets - countStored
        clusters = clusters.filter({ case (_: RoaringBitmap, extend: Boolean, _: RoaringBitmap, _: RoaringBitmap) => extend })
      }

      // Remove useless checkpoints from memory
      spark.sparkContext.getPersistentRDDs.keys.toVector.sorted.dropRight(1).foreach(id => spark.sparkContext.getPersistentRDDs(id).unpersist(true))
      printCluster(clusters)
    } while (countToExtend.value > 0)

    val res: Array[(RoaringBitmap, Int, Int)] =
      if (!returnResult) {
        Array()
      } else {
        clusters
          .filter(cluster => !cluster._2 && cluster._3.getCardinality >= minsup) // This filter is mandatory to obtain only the interesting cells
          .map(i => Array[(RoaringBitmap, Int, Int)]((i._1, i._1.getCardinality, i._3.getCardinality)))
          .fold(Array.empty[(RoaringBitmap, Int, Int)])(_ ++ _)
      }

    writeStatsToFile(outTable2, inTable, minsize, minsup, nItemsets, storage_thr, repfreq, limit, nexecutors, ncores, maxram, timeScale, unit_t, bin_t, eps_t, bin_s, eps_s, nTransactions, brdTrajInCell_bytes)
    spark.sparkContext.getPersistentRDDs.foreach(i => i._2.unpersist())
    spark.catalog.clearCache()
    spark.sqlContext.clearCache()
    (nItemsets, res)
  }

  val linesToPrint = 20
  var debug: Boolean = _
  var returnResult: Boolean = _
  var partitions: Int = _
  var inTable: String = _
  var conf: String = _
  var outTable: String = _
  var outTable2: String = _
  var transactionTable: String = _
  var cellToIDTable: String = _
  var neighborhoodTable: String = _
  var storage_thr: Int  = _
  var repfreq: Int  = _
  var limit: Int  = _
  var nexecutors: Int  = _
  var ncores: Int  = _
  var maxram: String  = _
  var timeScale: TemporalScale  = _
  var unit_t: Int = _
  var bin_t: Int = _
  var eps_t: Double = _
  var bin_s: Int = _
  var eps_s: Double = _
  var euclidean: Boolean = _

  // scalastyle:off
  /**
   * Run CTM
   *
   * @param inTable      Input table
   * @param debugData    (debug purpose) If non empty, use this instead of `inTable` (for debug purpose, see `TemporalTrajectoryFlowTest.scala`)
   * @param minsize      Minimum size of the co-movement patterns
   * @param minsup       Minimum support of the co-movement patterns (i.e., how long trajectories moved together)
   * @param storage_thr  Store co-movement patterns if they exceed this threshold.
   * @param repfreq      repfreq \in N. Repartition frequency (how frequently co-movement are repartitioned)
   * @param limit        Limit the size of the input dataset
   * @param nexecutors   Number of executors
   * @param ncores       Number of cores
   * @param maxram       Available RAM
   * @param timeScale    {"daily", "weekly", "absolute", "notime"}. Daily = Day hour (0, ..., 23). Weekly = Weekday (1, ..., 7).
   * @param unit_t       Time unit in seconds (1 = 1s, 3600 = 1h), map timestamp to the given temporal unit (ignored for weekly and daily)
   * @param bin_t        bin_t \in N (0, 1, ..., n). Size of the temporal quanta (expressed as a multiplier of timeBucketUnit). If 0, consider only a single bucket
   * @param eps_t        eps_t \in N (1, ..., n). Allow eps_t temporal neighbors
   * @param bin_s        bin_s \in N (1, ..., n). Multiply cell size 123m x bin_s
   * @param eps_s        eps_s \in N (1, ..., n). Allow eps_s spatial neighbors
   * @param returnResult (debug purpose) If True, return the results as a centralized array. Otherwise, store it to hive table
   * @param droptable    Whether the support table should be dropped and created brand new
   * @param euclidean    Whether the coordinates in the dataset are Polar or Euclidean
   * @return If `returnResult`, return the results as a centralized array. Otherwise, returns an empty array
   */
  def run(inTable: String = "foo", minsize: Int, minsup: Int,
          storage_thr: Int = STORAGE_THR, repfreq: Int = 1, limit: Int = Int.MaxValue, // EFFICIENCY PARAMETERS
          nexecutors: Int = NEXECUTORS, ncores: Int = NCORES, maxram: String = MAXRAM, // SPARK CONFIGURATION
          timeScale: TemporalScale, unit_t: Int = SECONDS_IN_HOUR, bin_t: Int = 1, eps_t: Double = Double.PositiveInfinity,
          bin_s: Int, eps_s: Double = Double.PositiveInfinity, // EFFECTIVENESS PARAMETERS
          debugData: Seq[(Tid, Vector[Itemid])] = Seq(), neighs: Map[Tid, RoaringBitmap] = Map(), spark: Option[SparkSession] = None, // INPUTS
          returnResult: Boolean = false,
          droptable: Boolean = false, euclidean: Boolean = false): (Long, Array[(RoaringBitmap, Int, Int)]) = {
    this.inTable = inTable
    this.debug = debugData.nonEmpty
    this.returnResult = returnResult
    this.partitions = nexecutors * ncores * 3
    this.storage_thr = storage_thr
    this.repfreq = repfreq
    this.limit = limit
    this.nexecutors = nexecutors
    this.ncores = ncores
    this.maxram = maxram
    this.timeScale = timeScale
    this.unit_t = SECONDS_IN_HOUR
    this.bin_t = 1
    this.eps_t = eps_t
    this.bin_s = bin_s
    this.eps_s = eps_s
    this.returnResult = returnResult
    this.euclidean = euclidean

    val temporaryTableName: String = // Define the generic name of the support table, based on the relevant parameters for cell creation.
      s"-tbl_${inTable.replace("trajectory.", "")}" +
        s"-lmt_${if (limit == Int.MaxValue) "Infinity" else limit}" +
        s"-size_$minsize" +
        s"-sup_$minsup" +
        s"-bins_$bin_s" +
        s"-ts_${timeScale.value}" +
        s"-bint_$bin_t" +
        s"-unitt_$unit_t"
    conf = // Define the generic name for the run, including temporary table name
      "CTM" + temporaryTableName +
        s"-epss_${if (eps_s.isInfinite) eps_s.toString else eps_s.toInt}" +
        s"-epst_${if (eps_t.isInfinite) eps_t.toString else eps_t.toInt}" +
        s"-freq_${repfreq}" +
        s"-sthr_${storage_thr}"

    outTable = conf.replace("-", "__")
    outTable2 = s"results/CTM_stats.csv"
    var neighbors: Map[Tid, RoaringBitmap] = Map()
    val trans: RDD[(Tid, Itemid)] =
      if (debug) {
        transactionTable = inTable
        spark.get.sparkContext.parallelize(debugData.flatMap({ case (cell: Tid, items: Vector[Itemid]) => items.map(item => (item, Array(cell))) }))
          .reduceByKey(_ ++ _, partitions)
          .flatMap(t => t._2.map(c => (c, t._1)))
          .cache()
      } else {
        /* ------------------------------------------------------------------------
         * Spark Environment Setup
         * ------------------------------------------------------------------------ */
          val spark: SparkSession = startSparkSession(conf, nexecutors, ncores, maxram, SPARK_SQL_SHUFFLE_PARTITIONS, "yarn")
          spark.sparkContext.setLogLevel("ERROR")
          Logger.getLogger("org").setLevel(Level.ERROR)
          Logger.getLogger("akka").setLevel(Level.ERROR)
          LogManager.getRootLogger.setLevel(Level.ERROR)
        /* *****************************************************************************************************************
         * CONFIGURING TABLE NAMES
         * *****************************************************************************************************************/

        transactionTable = s"tmp_transactiontable$temporaryTableName".replace("-", "__") // Trajectories mapped to cells
        cellToIDTable = s"tmp_celltoid$temporaryTableName".replace("-", "__") // Cells with ids
        neighborhoodTable = s"tmp_neighborhood$temporaryTableName".replace("-", "__") // Neighborhoods

        /* ***************************************************************************************************************
         * Trajectory mapping: mapping trajectories to trajectory abstractions
         * **************************************************************************************************************/
        println(s"\n--- Writing to \n\t$outTable\n\t$outTable2\n")
        if (droptable) {
          println("Dropping tables.")
          spark.sql(s"drop table if exists ${if (DB_NAME.nonEmpty) DB_NAME + "." else ""}$transactionTable")
          spark.sql(s"drop table if exists ${if (DB_NAME.nonEmpty) DB_NAME + "." else ""}$cellToIDTable")
          spark.sql(s"drop table if exists ${if (DB_NAME.nonEmpty) DB_NAME + "." else ""}$neighborhoodTable")
        }
        if (DB_NAME.nonEmpty) spark.sql(s"use $DB_NAME") // set the output trajectory database
        // the transaction table is only generated once, skip the generation if already generated
        if (!spark.catalog.tableExists(DB_NAME, transactionTable)) {
          // Create time bin from timestamp in a temporal table `inputDFtable`
          val inputDFtable = inTable.substring(Math.max(0, inTable.indexOf(".") + 1), inTable.length) + "_temp"
          println(s"--- Getting data from $inTable...")
          getData(spark, inTable, inputDFtable, timeScale, bin_t, unit_t, euclidean, bin_s)
          if (debug || returnResult) {
            spark.sql(s"select * from $inputDFtable limit $linesToPrint").show()
          }
          // create trajectory abstractions from the `inputDFtable`, and store it to the `transactionTable` table
          println(s"--- Generating $transactionTable...")
          mapToReferenceSystem(spark, inputDFtable, transactionTable, minsize, minsup, limit)
          spark.sql(s"select * from $transactionTable limit $linesToPrint").show()
          // create the quanta of the reference system from the `inputDFtable` temporal table, and store it to the `cellToIDTable`
          getQuanta(spark, transactionTable, cellToIDTable)
          spark.sql(s"select * from $cellToIDTable limit $linesToPrint").show()
          // create the table with all neighbors for each cell
          if (!eps_s.isInfinite || !eps_t.isInfinite) {
            println(s"--- Generating $neighborhoodTable...")
            createNeighborhood(spark, cellToIDTable, neighborhoodTable, timeScale, euclidean)
            spark.sql(s"select * from $neighborhoodTable limit $linesToPrint").show()
          }
        }

        val sql = s"select tid, itemid from $transactionTable"
        println(s"--- Input: $sql")
        spark
          .sql(sql)
          .rdd
          .map(i => (i.get(1).asInstanceOf[Int], Array(i.get(0).asInstanceOf[Int])))
      }
        .reduceByKey(_ ++ _, partitions)
        .flatMap(t => t._2.map(c => (c, t._1)))
        .cache()

    if (debug) {
      neighbors = neighs
    } else {
      /* *****************************************************************************************************************
       * BROADCASTING NEIGHBORHOOD
       * ****************************************************************************************************************/
      /** Define a neighborhood for each trajectoryID and broadcast it (only if a neighborhood metrics is defined).
       * Carpenter by default would not include this, thanks to this the algorithm will be able to use bounds on time and space */
      neighbors =
        if (!eps_s.isInfinite || !eps_t.isInfinite) {
          val spaceThreshold = if (eps_s.isInfinite) { None } else { Some(eps_s * bin_s * DEFAULT_CELL_SIDE) }
          val timeThreshold = if (eps_t.isInfinite) { None } else { Some(eps_t) }
          TemporalCellBuilder.broadcastNeighborhood(spark.get, spaceThreshold, timeThreshold, neighborhoodTable)
        } else {
          Map()
        }
    }
    val brdNeighborhood: Option[Broadcast[Map[Tid, RoaringBitmap]]] = if (neighbors.nonEmpty) { Some(spark.get.sparkContext.broadcast(neighbors)) } else { None }
    if (brdNeighborhood.isDefined) {
      val brdTrajInCell_bytes = brdNeighborhood.get.value.values.map(_.getSizeInBytes + 4).sum
      println(brdTrajInCell_bytes + "B")
      if (debug || returnResult) {
        brdNeighborhood.get.value.toList.sortBy(_._1).foreach(tuple => println(s"tid: ${tuple._1}, neighborhood: ${tuple._2}"))
      }
    }
    CTM2.CTM(spark.get, trans, brdNeighborhood, minsup, minsize)
  }

  /**
   * Main of the whole application.
   *
   * @param args arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new TemporalTrajectoryFlowConf(args)
    val debug: Boolean = conf.debug()
    if (args.nonEmpty && !debug) {
      run(
        inTable = conf.tbl(),
        euclidean = conf.euclidean(),
        droptable = conf.droptable(),
        timeScale = TemporalScale(conf.timescale.getOrElse(NoScale.value)),
        unit_t = conf.unitt.getOrElse(SECONDS_IN_HOUR),
        bin_t = conf.bint.getOrElse(0),
        eps_t = conf.epst(),
        eps_s = conf.epss(),
        bin_s = conf.bins(),
        nexecutors = conf.nexecutors(),
        ncores = conf.ncores(),
        maxram = conf.maxram(),
        storage_thr = conf.storagethr(),
        repfreq = conf.repfreq(),
        limit = conf.limit().toInt,
        minsize = conf.minsize(),
        minsup = conf.minsup()
      )
    } else {
      // TemporalTrajectoryFlowTest.runTest(conf.droptable())
    }
  }
}

/**
 * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
 *
 * @param arguments the programs arguments as an array of strings.
 */
class TemporalTrajectoryFlowConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val tbl = opt[String]()
  val limit = opt[Double]()
  val minsize = opt[Int]()
  val minsup = opt[Int]()
  val euclidean = opt[Boolean]()
  val bins = opt[Int]()
  val epss = opt[Double]()
  val repfreq = opt[Int]()
  val storagethr = opt[Int]()
  val nexecutors = opt[Int]()
  val ncores = opt[Int]()
  val maxram = opt[String]()
  val timescale = opt[String]()
  val unitt = opt[Int]()
  val bint = opt[Int]()
  val epst = opt[Double]()
  val debug = opt[Boolean](required = true)
  val droptable = opt[Boolean]()
  verify()
}