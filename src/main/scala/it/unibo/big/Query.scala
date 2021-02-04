package it.unibo.big

import it.unibo.big.TemporalScale.AbsoluteScale
import it.unibo.big.temporal.TemporalCellBuilder.{computeLatitudeQuery, computeLongitudeQuery, computeTimeBin}
import org.apache.spark.sql.{SaveMode, SparkSession}

/** This is the main class of the project */
object Query {
    /**
     * @param inTable Input table
     * @param minsize Minimum size of the co-movement patterns
     * @param minsup  Minimum support of the co-movement patterns (i.e., how long trajectories moved together)
     * @param bin_s   bin_s \in N (1, ..., n). Multiply cell size 123m x bin_s
     */
    def run(inTable: String, minsize: Int, minsup: Int, bin_s: Int, timescale: TemporalScale, bin_t: Int, euclidean: Boolean = false, exportRealData: Boolean = false): Unit = {
        val sparkSession = SparkSession.builder()
            .appName("Query trajectory")
            .master("yarn")
            .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
            .enableHiveSupport
            .getOrCreate
        var sql =
            s"""select i.itemsetid, t.itemid, t.tid, t.latitude as bin_latitude, t.longitude as bin_longitude, t.time_bucket as bin_timestamp, u.tileid as in_support ${if (exportRealData) ", s.userid, s.trajectoryid, s.`timestamp`, s.latitude, s.longitude " else ""}
               |from (select itemsetid, support from ctm.CTM__tbl_${inTable}__lmt_1000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}__unitt_1__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__summary order by 2 desc limit 2) a
               |     join ctm.CTM__tbl_${inTable}__lmt_1000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}__unitt_1__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__itemset i on (a.itemsetid = i.itemsetid)
               |     join ctm.tmp_transactiontable__tbl_${inTable}__lmt_1000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}__unitt_1 t on (t.itemid = i.itemid)
               |     left join ctm.CTM__tbl_${inTable}__lmt_1000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}__unitt_1__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__support u on (t.tid = u.tileid and i.itemsetid = u.itemsetid)
               |""".stripMargin
        if (exportRealData) {
            sql +=
                s"""
                   |    join trajectory.${inTable} s on (t.userid = s.userid and t.trajectoryid = s.trajectoryid
                   |        and ${computeTimeBin(timescale, bin_t, column = "s.`timestamp`")} = t.time_bucket
                   |        and ${computeLatitudeQuery(euclidean, bin_s, "s.latitude")} = t.latitude
                   |        and ${computeLongitudeQuery(euclidean, bin_s, "s.longitude")} = t.longitude)
                 """.stripMargin
        }
        val tablename = s"join__${inTable}__${minsize}__${minsup}__${bin_s}__${timescale.value}__${bin_t}"
        val hivequery = s"select itemsetid, itemid, tid${if (exportRealData) ", userid, trajectoryid, `timestamp`, latitude, longitude" else ""}, bin_latitude, bin_longitude, bin_timestamp, in_support from $tablename"
        val hivecommand = s"hive -e 'set hive.cli.print.header=true;use ctm; $hivequery' | sed 's/[\\t]/,/g'  > $tablename.csv"
        println(s"SQL:\n$sql")
        sparkSession.sql("use ctm")
        sparkSession.sql(sql).write.mode(SaveMode.Overwrite).saveAsTable(s"$tablename")
        sparkSession.stop()
        println(s"SQL:\n$sql")
        println(s"Hive command:\n$hivecommand")
        new ProcessBuilder(hivecommand).start().waitFor()
    }

    /**
     * Main of the whole application.
     *
     * @param args arguments
     */
    def main(args: Array[String]): Unit = {
        val conf = new Conf(args)
        run(
            inTable = conf.tbl.getOrElse("oldenburg_standard"),
            minsize = conf.minsize.getOrElse(1000),
            minsup = conf.minsup.getOrElse(20),
            bin_s = conf.bins.getOrElse(20),
            bin_t = conf.bint.getOrElse(5),
            timescale = TemporalScale(conf.timescale.getOrElse(AbsoluteScale.value)),
            euclidean = conf.euclidean.getOrElse(true)
        )
    }
}