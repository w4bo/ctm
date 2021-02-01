package it.unibo.big

import it.unibo.big.CTM._
import it.unibo.big.Utils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.roaringbitmap.RoaringBitmap

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable

object CTM2 {

    def CTM(spark: SparkSession, trans: RDD[(Tid, Itemid)], brdNeighborhood: Option[Broadcast[Map[Tid, RoaringBitmap]]], minsup: Int, minsize: Int, isPlatoon: Boolean): (Long, Array[(RoaringBitmap, Tid, Tid)]) = {
        if (trans.count() == 0) {
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
        // if (debug || returnResult) { brdTrajInCell.value.foreach(println) }
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
            val res = RoaringBitmap.bitmapOf()
            brdTrajInCell.value.foreach({ case (tid, transaction) =>
                if (prevSupport.isEmpty || !prevSupport.get.contains(tid)) {
                    val iterator = itemset.getIntIterator
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

        def connectedComponent(sp: RoaringBitmap, supp: RoaringBitmap, isPlatoon: Boolean, returnComponents: Boolean): (Int, RoaringBitmap) = {
            if (brdNeighborhood.isEmpty) {
                return (supp.getCardinality, supp)
            }
            var c = 0 // size of the connected component
            var isValidPlatoon = true // whether this is a valid platoon
            val marked: mutable.Set[Int] = mutable.Set() // explored neighbors
            var cc: RoaringBitmap = RoaringBitmap.bitmapOf() // current connected component
            val ccs: RoaringBitmap = RoaringBitmap.bitmapOf() // components accumulator
            var maxc = if (isPlatoon) Int.MaxValue else Int.MinValue
            sp.forEach(toJavaConsumer({ tile: Integer => { // for each tile in the support
                if ( // if a connected component of at least minsup has not been found && the tile has been not visited yet
                    (returnResult && (!isPlatoon || isValidPlatoon) || // to return the result, I cannot stop to minsup
                        !returnResult && (!isPlatoon && c < minsup || isPlatoon && isValidPlatoon)) && !marked.contains(tile)
                ) {
                    // reset the connected component
                    c = 0
                    cc = RoaringBitmap.bitmapOf()

                    def connectedComponentRec(i: Int): Unit = { // recursive function
                        marked += i // add the tile to the explored set
                        c += 1 // increase the number of adjacent tiles
                        if (returnResult) {
                            cc.add(i)
                        } // add the adjacent tile
                        // for each neighbor of the current tile, if the neighbor is in the cluster support and it has been not explored yet ...
                        val neighborhood: Option[RoaringBitmap] = brdNeighborhood.get.value.get(i)
                        // not all neighborhoods are defined (for instance due to the pruning of tiles without a sufficient amount of trajectories)
                        if (neighborhood.isDefined) {
                            neighborhood.get.forEach(toJavaConsumer(i =>
                                if ((returnResult || c < minsup) && !marked.contains(i) && supp.contains(i)) {
                                    connectedComponentRec(i)
                                }))
                        }
                    }

                    connectedComponentRec(tile)
                    isValidPlatoon = c >= minsup
                    // add the connected component if big enough
                    maxc = if (isPlatoon) Math.min(c, maxc) else Math.max(c, maxc)
                    if (returnResult && c >= minsup) {
                        ccs.or(cc)
                    }
                }
            }
            }))
            (maxc, ccs)
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
        def filterCluster(cluster: RoaringBitmap, x: RoaringBitmap, r: RoaringBitmap, trueSupport: RoaringBitmap): Boolean = {
            var flag: Boolean = cluster.getCardinality >= minsize && (x.getCardinality + r.getCardinality) >= minsup
            /* check if it is in the shortest path (i.e., contains the first location, and (sup \setminus r \setminus x) is empty */
            flag = flag && x.contains(trueSupport.getIntIterator.next()) &&
                // RoaringBitmap.andNot(RoaringBitmap.andNot(trueSupport, r), x).isEmpty
                RoaringBitmap.andNot(trueSupport, r).getCardinality == x.getCardinality
            flag
        }

        def toExtend(support: RoaringBitmap): Boolean = {
            val flag = support.getCardinality < minsup
            if (flag || brdNeighborhood.isEmpty) {
                return flag
            }
            val c = connectedComponent(support, support, isPlatoon = isPlatoon, returnComponents = false) // consecutive adjacent tiles
            c._1 < minsup
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
                    Array((lCluster, true, X, R, support(lCluster)))
                })
                .filter({ case (i: RoaringBitmap, extend: Boolean, cs: RoaringBitmap, sp: RoaringBitmap, ts: RoaringBitmap) => filterCluster(i, cs, sp, ts) })
                .localCheckpoint()
        val clusterCount = clusters.count()
        println(s"\n--- Init clusters: $clusterCount")
        countOk.reset()
        countToExtend.reset()
        /* *****************************************************************************************************************
         * END - Creating the transactional dataset
         * ****************************************************************************************************************/

        val empty: RoaringBitmap = RoaringBitmap.bitmapOf()
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
                    .flatMap({ case (lCluster: RoaringBitmap, extend: Boolean, x: RoaringBitmap, r: RoaringBitmap, lClusterSupport: RoaringBitmap) =>
                        // require(!RoaringBitmap.intersects(x, r), s"$x intersects $r")
                        if (extend) { // If the cluster should be extended...
                            var L: Array[CarpenterRowSet] = Array() // accumulator
                            // cannot check this after connectedComponent(RoaringBitmap.or(XplusY, R)), otherwise platoon fails
                            if (!toExtend(lClusterSupport)) { // if is a valid co-movement pattern...
                                countOk.add(1) // update the counter
                                countToExtend.add(-1) // decrease the counter to extend to avoid double counting (since later L.size will contain this pattern)
                                L +:= (lCluster, false, empty, empty, empty)
                            }
                            val Y: RoaringBitmap = RoaringBitmap.and(lClusterSupport, r) // tiles shared by all transactions
                            val XplusY: RoaringBitmap = RoaringBitmap.or(x, Y) // ... are directly added to the current support
                            var R: RoaringBitmap = RoaringBitmap.and(RoaringBitmap.andNot(r, Y), allCellsInTrajectories(lCluster)) // ... and removed from the search space
                            // if at least a connected component exists
                            val conncomp = connectedComponent(R, RoaringBitmap.or(XplusY, R), isPlatoon = false, returnComponents = true)
                            // R.toArray()
                            if (conncomp._1 >= minsup) {
                                // R = RoaringBitmap.and(R, conncomp._2)
                                // RoaringBitmap.and(R, conncomp._2)
                                R.forEach(toJavaConsumer({ key: Integer => {
                                    val c = RoaringBitmap.and(lCluster, brdTrajInCell.value(key)) // new co-movement pattern
                                    R = RoaringBitmap.remove(R, key, key + 1) // reduce the search space
                                    val XplusYplusKey = RoaringBitmap.add(XplusY, key, key + 1) // update the current support
                                    val newClusterSupport = support(c, Some(lClusterSupport))
                                    if (filterCluster(c, XplusYplusKey, R, newClusterSupport)) { // if is *potentially* a valid co-movement pattern...
                                        L +:= (c, true, XplusYplusKey, R, newClusterSupport) // ... store it
                                    }
                                }
                                }))
                                countToExtend.add(L.length)
                            }
                            L
                        } else {
                            if (!lCluster.hasRunCompression) lCluster.runOptimize()
                            Array((lCluster, extend, x, r, lClusterSupport))
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
            if (!returnResult && (countToExtend.value == 0 || nItemsets - countStored >= storage_thr)) {
                if (storage_thr > 0) {
                    println(s"--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} Writing itemsets to the database")
                    import spark.implicits._
                    val towrite: RDD[(Int, RoaringBitmap)] = clusters
                        .filter({ case (_: RoaringBitmap, extend: Boolean, _: RoaringBitmap, _: RoaringBitmap, _: RoaringBitmap) => !extend })
                        .map(cluster => (cluster._1.hashCode(), cluster._1))
                        .cache()

                    towrite
                        .map({ case (uid: Int, itemset: RoaringBitmap) =>
                            (uid, itemset.getCardinality, support(itemset, Option.empty).getCardinality)
                        })
                        .toDF("itemsetid", "size", "support")
                        .write.mode(if (countStored == 0) SaveMode.Overwrite else SaveMode.Append).saveAsTable(summaryTable)
                    require(!spark.sql(s"select * from $summaryTable").isEmpty, s"Empty $summaryTable")

                    towrite
                        .flatMap({ case (uid: Int, itemset: RoaringBitmap) =>
                            itemset.toArray.map(i => {
                                require(i >= 0, "itemid is below zero")
                                (uid, i)
                            })
                        })
                        .toDF("itemsetid", "itemid")
                        .write.mode(if (countStored == 0) SaveMode.Overwrite else SaveMode.Append).saveAsTable(itemsetTable)
                    require(!spark.sql(s"select * from $itemsetTable").isEmpty, s"Empty $itemsetTable")

                    towrite
                        .flatMap({ case (uid: Int, itemset: RoaringBitmap) =>
                            support(itemset, Option.empty).toArray.map(i => {
                                require(i >= 0, "tileid is below zero")
                                (uid, i)
                            })
                        })
                        .toDF("itemsetid", "tileid")
                        .write.mode(if (countStored == 0) SaveMode.Overwrite else SaveMode.Append).saveAsTable(supportTable)
                    require(!spark.sql(s"select * from $supportTable").isEmpty, s"Empty $supportTable")
                    println(s"--- ${new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(Calendar.getInstance().getTime)} Done writing")
                }
                countStored += nItemsets - countStored
                clusters = clusters.filter({ case (_: RoaringBitmap, extend: Boolean, _: RoaringBitmap, _: RoaringBitmap, _: RoaringBitmap) => extend })
            }
            // Remove useless checkpoints from memory
            spark.sparkContext.getPersistentRDDs.keys.toVector.sorted.dropRight(1).foreach(id => spark.sparkContext.getPersistentRDDs(id).unpersist(true))
        } while (countToExtend.value > 0)

        val res: Array[(RoaringBitmap, Int, Int)] =
            if (!returnResult) {
                Array()
            } else {
                clusters
                    // .filter({ case (_: RoaringBitmap, extend: Boolean, _: RoaringBitmap, _: RoaringBitmap) => !extend }) // This filter is mandatory to obtain only the interesting cells
                    .map({ case (i: RoaringBitmap, _: Boolean, _: RoaringBitmap, _: RoaringBitmap, _: RoaringBitmap) => Array[(RoaringBitmap, Int, Int)]((i, i.getCardinality, support(i).getCardinality)) })
                    .fold(Array.empty[(RoaringBitmap, Int, Int)])(_ ++ _)
            }
        writeStatsToFile(outTable2, inTable, minsize, minsup, nItemsets, storage_thr, repfreq, limit, nexecutors, ncores, maxram, timeScale, unit_t, bin_t, eps_t, bin_s, eps_s, nTransactions, brdTrajInCell.value.values.map(_.getSizeInBytes + 4).sum, if (brdNeighborhood.isEmpty) 0 else brdNeighborhood.get.value.values.map(_.getSizeInBytes + 4).sum)
        spark.sparkContext.getPersistentRDDs.foreach(i => i._2.unpersist())
        spark.catalog.clearCache()
        spark.sqlContext.clearCache()
        (nItemsets, res)
    }
}
