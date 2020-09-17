package it.unibo.big

import java.text.SimpleDateFormat
import java.util.Calendar

import it.unibo.big.CTM.{bin_s, bin_t, conf, debug, eps_s, eps_t, inTable, limit, linesToPrint, maxram, ncores, nexecutors, outTable, outTable2, partitions, repfreq, returnResult, storage_thr, timeScale, unit_t}
import it.unibo.big.Utils.{CarpenterRowSet, CustomTimer, Itemid, Tid, toJavaConsumer, writeStatsToFile}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.roaringbitmap.{RoaringArray, RoaringBitmap}

import scala.collection.mutable

object CTM2 {
  def CTM(spark: SparkSession, trans: RDD[(Tid, Itemid)], brdNeighborhood: Option[Broadcast[Map[Tid, RoaringBitmap]]], minsup: Int, minsize: Int, isPlatoon: Boolean): (Long, Array[(RoaringBitmap, Tid, Tid)]) = {
    import spark.implicits._
    if (trans.count() == 0) {
      println("Empty transactions")
      return (0, Array())
    }

    /* *****************************************************************************************************************
     * BROADCASTING TRAJECTORIES IN CELL. This is the TT' used as transposed table as defined by Carpenter algorithm.
     * Also use RoaringbitMap for performance reason
     * ****************************************************************************************************************/
    // print("--- Broadcasting brdTrajInCell (i.e., itemInTid)... ")
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
    // if (debug || returnResult) {
    //   brdTrajInCell.value.foreach(println)
    // }
    /* *****************************************************************************************************************
     * END - BROADCASTING TRAJECTORIES IN CELL
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
    def support(itemset: RoaringBitmap, prevSupport: Option[RoaringBitmap] = None): RoaringBitmap = {
      //      RoaringBitmap.bitmapOf(brdTrajInCell.value.filter({ case (_, transaction) =>
      //        val iterator = itemset.iterator()
      //        var isOk = true
      //        while (iterator.hasNext && isOk) {
      //          isOk = transaction.contains(iterator.next())
      //        }
      //        isOk
      //      }).keys.toSeq: _*)
      val res = RoaringBitmap.bitmapOf()
      brdTrajInCell.value.foreach({ case (tid, transaction) =>
        if (!prevSupport.isDefined || !prevSupport.get.contains(tid)) {
          val iterator = itemset.iterator()
          var isOk = true
          while (isOk && iterator.hasNext) {
            isOk = transaction.contains(iterator.next())
          }
          if (isOk) {
            res.add(tid)
          }
        } else {
          res.add(tid)
        }
      })
      res
    }

    def connectedComponent(support: RoaringBitmap): Int = {
      if (brdNeighborhood.isEmpty) { return support.getCardinality }
      var c = 0
      var isValidPlatoon = true
      val marked: mutable.Set[Int] = mutable.Set() // explored neighbors
      support.forEach(toJavaConsumer({ case tile: Integer => { // for each tile in the support
        if ((!isPlatoon && c < minsup || isPlatoon && isValidPlatoon) && !marked.contains(tile)) { // if a connected component of at least minsup has not been found && the tile has been not visited yet
          c = 0 // reset the number of consecutive adjacent tiles
          def connectedComponentRec(i: Int): Unit = { // recursive function
            marked += i // add the tile to the explored set
            c += 1 // increase the number of adjacent tile
            // for each neighbor of the current tile, if the neighbor is in the cluster support and it has been not explored yet ...
            val neighborhood: Option[RoaringBitmap] = brdNeighborhood.get.value.get(i)
            // not all neighborhoods are defined (for instance due to the pruning of tiles without a sufficient amount of trajectories)
            if (neighborhood.isDefined) {
              neighborhood.get.forEach(toJavaConsumer(i => if (c < minsup && support.contains(i) && !marked.contains(i)) {
                connectedComponentRec(i)
              }))
            }
          }
          connectedComponentRec(tile)
          isValidPlatoon = c >= minsup
        }
      }}))
      c
    }

    /**
     * Keep only the cluster having
     * (i) a sufficient size
     * (ii) a sufficient support
     * (iii) emerging from a non-redundant path
     * (iv) satisfying the distance/adjacency constraints (i.e., neighborhood constraints)
     *
     * @param cluster the current trajectory set considered.
     * @param x       a set containing the current support of the trajectory set.
     * @param r       a set containing all possible cellset to be added to x.
     * @return true if the trajectory set is on its maximal path and has good size and support, false otherwise.
     */
    def filterCluster(cluster: RoaringBitmap, x: RoaringBitmap, r: RoaringBitmap, prevSupport: Option[RoaringBitmap] = None): Boolean = {
      var flag: Boolean = cluster.getCardinality >= minsize && (x.getCardinality + r.getCardinality) >= minsup
      val trueSupport = support(cluster, prevSupport) // get the support
      if (flag) { // if the itemset has enough support and enough elements
        /* check if it is in the shortest path (i.e., contains the first location, and (sup \setminus r \setminus x) is empty */
        flag = flag && x.contains(trueSupport.getIntIterator.next()) &&
                 // RoaringBitmap.andNot(RoaringBitmap.andNot(trueSupport, r), x).isEmpty
                 RoaringBitmap.andNot(trueSupport, r).getCardinality == x.getCardinality
      }
      //      if (flag && brdNeighborhood.isDefined) {
      //        var c = 0 // consecutive adjacent tiles
      //        var isValidPlatoon = true
      //        val marked: mutable.Set[Int] = mutable.Set() // explored neighbors
      //        trueSupport.forEach(toJavaConsumer({ case tile: Integer => { // for each tile in the support
      //          if ((!isPlatoon && c < minsup || isPlatoon && isValidPlatoon) && !marked.contains(tile)) { // if a connected component of at least minsup has not been found && the tile has been not visited yet
      //            c = 0 // reset the number of consecutive adjacent tiles
      //            def searchConnectedComponent(i: Int): Unit = { // recursive function
      //              marked += i // add the tile to the explored set
      //              c += 1 // increase the number of adjacent tile
      //              // for each neighbor of the current tile, if the neighbor is in the cluster support and it has been not explored yet ...
      //              val neighborhood: Option[RoaringBitmap] = brdNeighborhood.get.value.get(i)
      //              // not all neighborhoods are defined (for instance due to the pruning of tiles without a sufficient amount of trajectories)
      //              if (neighborhood.isDefined) {
      //                neighborhood.get.forEach(toJavaConsumer(i => if (c < minsup && trueSupport.contains(i) && !marked.contains(i)) { searchConnectedComponent(i) }))
      //              }
      //            }
      //            searchConnectedComponent(tile)
      //            isValidPlatoon = c >= minsup
      //          }
      //        }}))
      //        flag = flag && c >= minsup
      //      }
      flag
    }

    def toExtend(support: RoaringBitmap): Boolean = {
      val flag = support.getCardinality < minsup
      if (flag || brdNeighborhood.isEmpty) {
        return flag
      }
      val c = connectedComponent(support) // consecutive adjacent tiles
      c < minsup
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

    CustomTimer.start()
    var clusters: RDD[CarpenterRowSet] =
      trans
        .mapValues({ trajId: Itemid => RoaringBitmap.bitmapOf(trajId) }) // This map the CellID -> TrajID converting the TrajID into a RoraingBitMap
        .reduceByKey((aTrajectory, bTrajectory) => RoaringBitmap.or(aTrajectory, bTrajectory)) // group by all the trajectories in the same cell
        .flatMap({ case (key: Tid, lCluster: RoaringBitmap) => // Now this is a Real TT' as intended by John Carpenter itself
          val X: RoaringBitmap = RoaringBitmap.bitmapOf(key) // current support
          val R: RoaringBitmap = RoaringBitmap.bitmapOf(brdTrajInCell.value.filter({ case (otherKey: Tid, _: RoaringBitmap) => key < otherKey }).toArray.map(_._1.toInt): _*) // select only the new cells
          Array((lCluster, true, X, R))
        })
        .localCheckpoint()
    val clusterCount = clusters.count()
    println(s"\n--- Init clusters: $clusterCount")
    countOk.reset()
    countToExtend.reset()
    // printCluster(clusters)
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
        (if (curIteration % repfreq == 0) { clusters.repartition(partitions) } else { clusters }) // Shuffle the data every few iterations
          .flatMap({ case (lCluster: RoaringBitmap, extend: Boolean, x: RoaringBitmap, r: RoaringBitmap) =>
            // require(!RoaringBitmap.intersects(x, r), s"$x intersects $r")
            if (extend) { // If the cluster should be extended...
              val lClusterSupport = support(lCluster)
              val Y: RoaringBitmap = RoaringBitmap.and(lClusterSupport, r) // TIDs in all the rows
              val XplusY: RoaringBitmap = RoaringBitmap.or(x, Y) // X U Y = x + Y
              var R: RoaringBitmap = RoaringBitmap.and(RoaringBitmap.andNot(r, Y), allCellsInTrajectories(lCluster)) // rows to consider
              val t = (lCluster, toExtend(lClusterSupport), XplusY, R)
              var L: Array[CarpenterRowSet] = if (t._2) Array[CarpenterRowSet]() else Array(t)
              if (connectedComponent(RoaringBitmap.or(XplusY, R)) >= minsup) {
                /* Cluster t should be expanded */
                R.toArray.foreach({ case key: Tid => {
                  val rTransaction = brdTrajInCell.value(key)
                  val c = RoaringBitmap.and(lCluster, rTransaction) // C stores the common trajectories between the tuple Key and the lcluster
                  // c.runOptimize() // RUN LENGTH ENCODING TO SAVE MEMORY
                  R = RoaringBitmap.remove(R, key, key + 1) // Remove the key element from previous defined R
                  val XplusYplusKey = RoaringBitmap.add(XplusY, key, key + 1)
                  /* c becomes new lcluster, Add the element key to X */
                  L +:= (c, true, XplusYplusKey, R)
                }
                })
              }
              L = L.filter({ case (c: RoaringBitmap, _: Boolean, x: RoaringBitmap, r: RoaringBitmap) => !x.isEmpty && filterCluster(c, x, r, Some(lClusterSupport)) })
              // the current element has already been counted, if it has to be saved don't count it twice
              // do not also count the elements that will be checked in the next phase (i.e., for which filter returns true)
              L.foreach(t => if (t._2) { countToExtend.add(1) } else { if (t._3.getCardinality >= minsup) countOk.add(1) })
              L
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
      // printCluster(clusters)
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
}
