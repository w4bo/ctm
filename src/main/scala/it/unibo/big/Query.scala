package it.unibo.big

import it.unibo.big.TemporalScale.AbsoluteScale
import it.unibo.big.Utils.startSparkSession
import it.unibo.big.temporal.TemporalCellBuilder.{computeLatitudeQuery, computeLongitudeQuery, computeTimeBin}
import org.apache.spark.sql.SaveMode

import java.io._

/** This is the main class of the project */
object Query {

    /**
     * Main of the whole application.
     *
     * @param args arguments
     */
    def main(args: Array[String]): Unit = {
        val conf = new Conf(args)
        run(
            inTable = conf.tbl.getOrElse("inTable"),
            minsize = conf.minsize.getOrElse(1000),
            minsup = conf.minsup.getOrElse(20),
            bin_s = conf.bins.getOrElse(20),
            bin_t = conf.bint.getOrElse(5),
            timescale = TemporalScale(conf.timescale.getOrElse(AbsoluteScale.value)),
            euclidean = conf.euclidean.getOrElse(true),
            querytype = conf.querytype()
        )
    }

    /**
     * @param inTable Input table
     * @param minsize Minimum size of the co-movement patterns
     * @param minsup  Minimum support of the co-movement patterns (i.e., how long trajectories moved together)
     * @param bin_s   bin_s \in N (1, ..., n). Multiply cell size 123m x bin_s
     */
    def run(inTable: String, minsize: Int, minsup: Int, bin_s: Int, timescale: TemporalScale, bin_t: Int, euclidean: Boolean, querytype: String): Unit = {
        querytype match {
            case "export" => exportData(inTable, minsize, minsup, bin_s, timescale, bin_t, euclidean)
        }
    }

    /**
     * Export original and binned data from the valid patterns
     */
    def exportData(inTable: String, minsize: Int, minsup: Int, bin_s: Int, timescale: TemporalScale, bin_t: Int, euclidean: Boolean, exportRealData: Boolean = true): Int = {
        val sparkSession = startSparkSession(appName = "QueryCTM", master = "yarn")
        var sql =
            s"""select i.itemsetid, t.itemid, t.tid, t.latitude as bin_latitude, t.longitude as bin_longitude, t.time_bucket as bin_timestamp, u.tileid as in_support ${if (exportRealData) ", s.userid, s.trajectoryid, s.`timestamp`, s.latitude, s.longitude " else ""}
               |from (select itemsetid, support from ctm.CTM__tbl_${inTable}__lmt_1000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__summary order by 2 desc limit 2) a
               |     join ctm.CTM__tbl_${inTable}__lmt_1000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__itemset i on (a.itemsetid = i.itemsetid)
               |     join ctm.tmp_transactiontable__tbl_${inTable}__lmt_1000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t} t on (t.itemid = i.itemid)
               |     left join ctm.CTM__tbl_${inTable}__lmt_1000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__support u on (t.tid = u.tileid and i.itemsetid = u.itemsetid)
               |""".stripMargin
        if (exportRealData) {
            sql +=
                s"""     join trajectory.${inTable} s on (t.userid = s.userid and t.trajectoryid = s.trajectoryid
                   |        and ${computeTimeBin(timescale, bin_t, column = "s.`timestamp`")} = t.time_bucket
                   |        and ${computeLatitudeQuery(euclidean, bin_s, "s.latitude")} = t.latitude
                   |        and ${computeLongitudeQuery(euclidean, bin_s, "s.longitude")} = t.longitude)
                 """.stripMargin
        }
        var tablename = s"join__${inTable}__${minsize}__${minsup}__${bin_s}__${timescale.value}__${bin_t}"

        println(s"SQL:\n$sql")
        sparkSession.sql("use ctm")
        sparkSession.sql(sql).write.mode(SaveMode.Overwrite).saveAsTable(tablename)


        val pw = new PrintWriter(new File("saveQuery.sh"))
        var hivequery = s"select itemsetid, itemid, tid${if (exportRealData) ", userid, trajectoryid, `timestamp`, latitude, longitude" else ""}, bin_latitude, bin_longitude, bin_timestamp, in_support from $tablename"
        var hiveCommand = s"hive -e 'set hive.cli.print.header=true; use ctm; $hivequery' | sed 's/[\\t]/,/g'  > $tablename.csv"
        println(s"Hive command:\n$hiveCommand")
        pw.write(hiveCommand)

        // sparkSession.sql(s"select itemid, bin_latitude, bin_longitude, bin_timestamp from $tablename").rdd.map(r => (r.get(3), (r.get(0), r.get(1), r.get(2))))
        hivequery = s"select itemid, bin_latitude, bin_longitude, bin_timestamp from $tablename"
        hiveCommand = s"hive -e 'set hive.cli.print.header=true; use ctm; $hivequery' > $tablename.tsv"
        var hdfsCommand = s"hdfs dfs -put $tablename.tsv /user/mfrancia/spare/input"
        println(s"Hive command:\n$hiveCommand")
        pw.write(hiveCommand)
        pw.write(hdfsCommand)

        tablename = s"tmp_transactiontable__tbl_${inTable}__lmt_1000000__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}"
        hivequery = s"select itemid, latitude, longitude, time_bucket from $tablename"
        hiveCommand = s"hive -e 'set hive.cli.print.header=true; use ctm; $hivequery' > $tablename.tsv"
        hdfsCommand = s"hdfs dfs -put $tablename.tsv /user/mfrancia/spare/input"
        println(s"Hive command:\n$hiveCommand")
        pw.write(hiveCommand)
        pw.write(hdfsCommand)

        // df.repartition(1).write.option("sep", ",").option("header", "true").option("escape", "\"").option("quoteAll", "true").csv("file:////home/mfrancia/ctm/output/")
        sparkSession.stop()
        pw.close()

        new ProcessBuilder("./saveQuery.sh").start().waitFor()
    }
}