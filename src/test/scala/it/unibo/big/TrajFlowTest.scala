package it.unibo.big

import org.apache.log4j.Logger
import org.apache.spark.HashPartitioner
import org.junit.Assert._
import org.junit.Test
import org.roaringbitmap.RoaringBitmap
import org.scalatest.junit.JUnitSuite

import java.util.Random

class TrajFlowTest extends JUnitSuite {
    private val L = Logger.getLogger(classOf[TrajFlowTest])

    @Test def roaringPerformance1()(): Unit = {
        val others = (0 until 1500)
        val a: RoaringBitmap = RoaringBitmap.bitmapOf((0 to 5000000): _*)
        var prev = a.getSizeInBytes
        require(a.runOptimize())
        var rle = a.getSizeInBytes
        assertTrue(prev > rle)

        var r = new Random(3)
        val b: RoaringBitmap = RoaringBitmap.bitmapOf((0 to 5000000).filter(_ => r.nextInt(10) >= 5): _*)
        prev = b.getSizeInBytes
        b.runOptimize()
        rle = b.getSizeInBytes
        assertEquals(prev, rle)

        r = new Random(3)
        val c: RoaringBitmap = RoaringBitmap.bitmapOf((0 to 5000000).filter(_ => r.nextInt(10) >= 1): _*)
        prev = c.getSizeInBytes
        c.runOptimize()
        rle = c.getSizeInBytes
        assertTrue(prev > rle)
    }

    @Test def roaringPerformance3(): Unit = {
        val r: Random = new Random(1)
        val brdTrajInCell: Map[Int, RoaringBitmap] = (1 to 5000).map(i => i -> {
            val rr = RoaringBitmap.bitmapOf((1 to 20000).map(_ => r.nextInt(300000)).toArray: _*)
            rr.runOptimize()
            rr
        }).toMap
        val itemset = RoaringBitmap.bitmapOf((1 to 20000).map(_ => r.nextInt(300000)).toArray: _*)
        var startTime: Long = System.currentTimeMillis()

        var contains = 0
        startTime = System.currentTimeMillis()
        val res5 = RoaringBitmap.bitmapOf(brdTrajInCell.filter({ case (_, transaction) =>
            val iterator = itemset.getIntIterator
            var isOk = true
            while (iterator.hasNext && isOk) {
                isOk = transaction.contains(iterator.next())
                contains += 1
            }
            isOk
        }).keys.toSeq: _*)
        val time5 = System.currentTimeMillis() - startTime
        L.debug(s"Sol hybrid 2... time: ${time5}, contains: $contains")

        contains = 0

        def fun(): Seq[Int] = {
            val iterator = itemset.getIntIterator
            var toiterate = brdTrajInCell
            while (iterator.hasNext) {
                val currentTrajectory = iterator.next()
                toiterate = toiterate.filter(t => {
                    contains += 1
                    t._2.contains(currentTrajectory)
                })
            }
            toiterate.keys.toSeq
        }

        startTime = System.currentTimeMillis()
        val res6 = RoaringBitmap.bitmapOf(fun(): _*)
        val time6 = System.currentTimeMillis() - startTime
        L.debug(s"Sol hybrid 4... time: ${time6}, contains: $contains")

        startTime = System.currentTimeMillis()
        val res4 = RoaringBitmap.bitmapOf(brdTrajInCell.filter({ case (_, transaction) =>
            val xs = itemset.toArray
            var isOk = true
            var idx = 0
            while (isOk && idx < xs.length) {
                isOk = transaction.contains(xs(idx))
                idx += 1
            }
            isOk
        }).keys.toSeq: _*)
        val time4 = System.currentTimeMillis() - startTime
        L.debug(s"Sol hybrid array... $time4")

        startTime = System.currentTimeMillis()
        val len = itemset.getCardinality
        val res1 = RoaringBitmap.bitmapOf(brdTrajInCell.filter({ case (_, transaction) => RoaringBitmap.and(itemset, transaction).getCardinality == len }).keys.toSeq: _*)
        val time1 = System.currentTimeMillis() - startTime
        L.debug(s"Sol and...  $time1")

        startTime = System.currentTimeMillis()
        val res2 = RoaringBitmap.bitmapOf(brdTrajInCell.filter({ case (_, transaction) => RoaringBitmap.andNot(itemset, transaction).isEmpty }).keys.toSeq: _*)
        val time2 = System.currentTimeMillis() - startTime
        L.debug(s"Sol andnot...  $time2")

        assertEquals(res1, res2)
        assertEquals(res1, res4)
        assertEquals(res1, res5)
        assertEquals(res1, res6)
    }

    @Test def roaringPerformance2()(): Unit = {
        val a: RoaringBitmap = RoaringBitmap.bitmapOf(0, 1, 4, 7, 8)
        val b: RoaringBitmap = RoaringBitmap.add(a, 2, 3)
        assertEquals(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7, 8), b)

        def next(k: Short, r: RoaringBitmap): Short = {
            r.toArray.filter(_ >= k).zipWithIndex.takeWhile(x => x._1 - k == x._2).maxBy(_._1)._1.toShort
        }

        assertTrue(Vector(Vector()).flatten.isEmpty)
        assertEquals(RoaringBitmap.bitmapOf(9), RoaringBitmap.andNot(RoaringBitmap.bitmapOf(7, 8, 9), a))
        assertEquals(3, (0 to 10).take(3).length)
        assertEquals(3, (0 to 10).take(Vector(1, 2, 3).size).length)
        assertEquals(2, next(0, b))
        assertEquals(2, next(1, b))
        assertEquals(2, next(2, b))
        assertEquals(4, next(4, b))
        assertEquals(8, next(7, b))
        assertEquals(4, RoaringBitmap.remove(a, 1, 2).getCardinality)
    }

    @Test def balancedPartitioner(): Unit = {
        val c1 = new DumbPartitioner(2, Map(1 -> 10, 2 -> 10, 3 -> 10))
        val c2 = new SortedPartitioner(2, Map(1 -> 10, 2 -> 10, 3 -> 10))
        assert(c2.partitions.keys.size == 3)
        assert(c2.partitions.values.toSet.size == 2)
        assert(c1.partitions.keys.size == 3)
        assert(c1.partitions.values.toSet.size == 2)
        assertEquals(Map(1 -> 0, 2 -> 0, 3 -> 1), c1.partitions)
        assertEquals(Map(1 -> 0, 2 -> 1, 3 -> 0), c2.partitions)
        assertEquals(1, c1.defaultPartition)
        assertEquals(1, c2.defaultPartition)
    }

    @Test def unbalancedPartitioner(): Unit = {
        val c1 = new DumbPartitioner(4, Map(0 -> 10000, 1 -> 10, 2 -> 10, 3 -> 10))
        val c2 = new SortedPartitioner(4, Map(0 -> 10000, 1 -> 10, 2 -> 10, 3 -> 10))
        assertEquals(Map(0 -> 0, 1 -> 1, 2 -> 1, 3 -> 1), c1.partitions)
        assertEquals(Map(0 -> 10000, 1 -> 30), c1.cumPartition)
        assertEquals(Map(0 -> 1, 1 -> 3), c1.cumKeyPartition)
        assertEquals(Map(0 -> 10000, 1 -> 10, 2 -> 10, 3 -> 10), c2.cumPartition)
        assertEquals(Map(0 -> 1, 1 -> 1, 2 -> 1, 3 -> 1), c2.cumKeyPartition)
        assertEquals(Map(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3), c2.partitions)
        assertEquals(3, c1.defaultPartition)
        assertEquals(3, c2.defaultPartition)
    }

    @Test def hashPartitioner(): Unit = {
        val p = new HashPartitioner(4)
        // Partitioning strategy: key.hashCode % nPartitions
        assertEquals(Map(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 0), Map(0 -> 10000, 1 -> 10, 2 -> 10, 3 -> 10, 4 -> 10).map(i => i._1 -> p.getPartition(i._1)))
    }

    @Test def localPartitioner(): Unit = {
        var p = new LocalPartitioner(2, Set(1, 2, 3, 4))
        assertEquals(Map(1 -> 0, 2 -> 0, 3 -> 1, 4 -> 1), Map(1 -> 10000, 2 -> 10, 3 -> 10, 4 -> 10).map(i => i._1 -> p.getPartition(i._1)))

        p = new LocalPartitioner(2, Set(11, 12, 13, 14))
        assertEquals(Map(11 -> 0, 12 -> 0, 13 -> 1, 14 -> 1), Map(11 -> 10000, 12 -> 10, 13 -> 10, 14 -> 10).map(i => i._1 -> p.getPartition(i._1)))

        p = new LocalPartitioner(2, Set(11, 12, 13))
        assertEquals(Map(11 -> 0, 12 -> 0, 13 -> 1), Map(11 -> 10000, 12 -> 10, 13 -> 10).map(i => i._1 -> p.getPartition(i._1)))
    }

    @Test def agg(): Unit = {
        val agg1 = new StatsAccumulator()
        agg1.add((1, 1, 1, 1L, 1L, 1L))
        agg1.add((2, 2, 2, 0L, 0L, 1L))
        assertEquals((3, 2, 1, 1, 1, 2), agg1.value)
        agg1.reset()
        assertTrue(agg1.isZero)
        agg1.add((1, 1, 1, 1L, 1L, 1L))
        assertEquals((1, 1, 1, 1, 1, 1), agg1.value)

        val agg2 = new CustomAccumulator()
        agg2.add(1)
        agg2.add(1)
        agg2.add(2)
        assertEquals(Map(1 -> 2, 2 -> 1), agg2.value)
        agg2.reset()
        assertTrue(agg2.isZero)
        agg2.add(1)
        assertEquals(Map(1 -> 1), agg2.value)
    }

    @Test def roaringbitmap(): Unit = {
        val r1 = RoaringBitmap.bitmapOf(1000, 120000, 300000)
        val r2 = RoaringBitmap.bitmapOf(1, 2, 300000)
        val r3 = RoaringBitmap.and(r1, r2)
        assertEquals(Seq(1000, 120000, 300000), r1.toArray.toSeq)
        assertEquals(Seq(1, 2, 300000), r2.toArray.toSeq)
        assertEquals(Seq(300000), r3.toArray.toSeq)

        val r4 = RoaringBitmap.bitmapOf((1 to 120000).toArray: _*)
        val r5 = RoaringBitmap.bitmapOf((60000 to 240000).toArray: _*)
        val r6 = RoaringBitmap.and(r4, r5)
        assert(r4.toArray.forall(v => v >= 0))
        assert(r5.toArray.forall(v => v >= 0))
        assert(r6.toArray.forall(v => v >= 0))

        assert(120000.shortValue < 0)
        assert(65535.shortValue == -1)
        assert(32767.shortValue == 32767)

        val r7 = RoaringBitmap.bitmapOf((1 to 12000000).toArray: _*)
        assert(r7.toArray.forall(v => v >= 0))
        assert(r7.getCardinality == 12000000)
    }
}
