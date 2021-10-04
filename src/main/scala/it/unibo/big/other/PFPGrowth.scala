package it.unibo.big.other

import it.unibo.big.{Conf, Main}
import it.unibo.big.Utils.{SPARK_SQL_SHUFFLE_PARTITIONS, startSparkSession}
import it.unibo.big.temporal.TemporalScale
import it.unibo.big.temporal.TemporalScale.NoScale
import org.apache.spark.ml.fpm.FPGrowth
object PFPGrowth {
    def main(args: Array[String]): Unit = {
        val conf = new Conf(args)
        val sparkSession = startSparkSession("PFPGrowth", conf.nexecutors(), conf.ncores(), conf.maxram(), SPARK_SQL_SHUFFLE_PARTITIONS, "yarn")
        import sparkSession.implicits._

        val card = sparkSession.sql(s"select distinct tid from tmp_transactiontable${Main.getDefaultTableName(conf.tbl(), conf.minsize(), conf.minsup(), conf.limit(), TemporalScale(conf.timescale.getOrElse(NoScale.value)), conf.bint(), conf.bins(), conf.additionalfeatures()) }").count()

        val dataset = sparkSession
            .sql(s"select tid, itemid from ${conf.tbl()}")
            .rdd
            .map(r => (r.get(0).toString.toInt, r.get(1).toString.toInt))
            .groupBy({ case (tid: Int, _: Int) => tid })
            .map({ case (_: Int, items) => items.map(_._2).toSeq })
            .toDF("items")

        val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(1.0 * conf.minsup() / card).setMinConfidence(0.00001)
        val model = fpgrowth.fit(dataset)
        println("Itemsets: " + model.freqItemsets.count())
        // // Display frequent itemsets.
        // model.freqItemsets.show()
        // // Display generated association rules.
        // model.associationRules.show()
        // // transform examines the input items against all the association rules and summarize the
        // // consequents as prediction
        // model.transform(dataset).show()
    }
}
