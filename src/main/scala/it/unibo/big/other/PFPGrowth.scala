package it.unibo.big.other

import it.unibo.big.{Conf, Main}
import it.unibo.big.Utils.{CustomTimer, SPARK_SQL_SHUFFLE_PARTITIONS, startSparkSession}
import it.unibo.big.temporal.TemporalScale
import it.unibo.big.temporal.TemporalScale.NoScale
import org.apache.spark.ml.fpm.FPGrowth

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

object PFPGrowth {
    def main(args: Array[String]): Unit = {
        val conf = new Conf(args)
        val sparkSession = startSparkSession("PFPGrowth", conf.nexecutors(), conf.ncores(), conf.maxram(), SPARK_SQL_SHUFFLE_PARTITIONS, "yarn")
        sparkSession.sparkContext.setCheckpointDir("pfpgrowth-checkpoints/")
        import sparkSession.implicits._
        val tablename = s"ctm.tmp_transactiontable${Main.getDefaultTableName(conf.tbl(), conf.minsize(), conf.minsup(), conf.limit(), TemporalScale(conf.timescale.getOrElse(NoScale.value)), conf.bint(), conf.bins(), conf.additionalfeatures.getOrElse(List()))}".replace("-", "__")
        val card = sparkSession.sql(s"select distinct tid from $tablename").count()
        val dataset = sparkSession
            .sql(s"select tid, itemid from $tablename")
            .rdd
            .map(r => (r.get(0).toString.toInt, r.get(1).toString.toInt))
            .groupBy({ case (tid: Int, _: Int) => tid })
            .map({ case (_: Int, items) => items.map(_._2).toSeq })
            .toDF("items")

        println("Started")
        CustomTimer.start()
        val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(1.0 * conf.minsup() / card).setMinConfidence(0.00001)
        val model = fpgrowth.fit(dataset)
        val itemsets = model.freqItemsets.count()
        writeStats(conf, itemsets)
        println("Done. Itemsets: " + itemsets)
        // // Display frequent itemsets.
        // model.freqItemsets.show()
        // // Display generated association rules.
        // model.associationRules.show()
        // // transform examines the input items against all the association rules and summarize the
        // // consequents as prediction
        // model.transform(dataset).show()
    }

    def writeStats(conf: Conf, nItemsets: Long) = {
        val fileName = "results/PFPGrowth_stats.csv"
        val fileExists = Files.exists(Paths.get(fileName))
        val outputFile = new File(fileName)
        outputFile.createNewFile()
        val bw = new BufferedWriter(new FileWriter(fileName, fileExists))
        if (!fileExists) {
            bw.write("time(ms),inTable,minsize,minsup,nItemsets,repfreq,limit,nexecutors,ncores,maxram,timescale,bin_t,eps_t,bin_s,eps_s\n".replace("_", "").toLowerCase)
        }
        bw.write(s"${CustomTimer.getElapsedTime},${conf.tbl()},${conf.minsize()},${conf.minsup()},$nItemsets,${conf.repfreq()},${conf.limit()},${conf.nexecutors()},${conf.ncores()},${conf.maxram()},${conf.timescale()},${conf.bint()},${conf.epst()},${conf.bins()},${conf.epss()}\n")
        bw.close()
    }
}
