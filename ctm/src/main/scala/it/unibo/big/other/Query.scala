package it.unibo.big.other

import it.unibo.big.Utils._
import it.unibo.big.temporal.TemporalCellBuilder._
import it.unibo.big.temporal.TemporalScale
import it.unibo.big.temporal.TemporalScale._
import org.apache.spark.sql.SaveMode
import org.rogach.scallop.ScallopConf

import java.io.{File, PrintWriter}
import java.util.Scanner

/** This is the main class of the project */
object Query {


    /**
     * Main of the whole application.
     *
     * @param args arguments
     */
    def main(args: Array[String]): Unit = {
        val conf = new Conf2(args)
        run(
            inTable = conf.tbl.getOrElse("inTable").replace("trajectory.", ""),
            minsize = conf.minsize.getOrElse(1000),
            minsup = conf.minsup.getOrElse(20),
            bin_s = conf.bins.getOrElse(20),
            bin_t = conf.bint.getOrElse(5),
            eps_t = conf.epst.getOrElse(Double.PositiveInfinity),
            eps_s = conf.epss.getOrElse(Double.PositiveInfinity),
            timescale = TemporalScale(conf.timescale.getOrElse(AbsoluteScale.value)),
            euclidean = conf.euclidean.getOrElse(true),
            querytype = conf.querytype.getOrElse("export"),
            limit = conf.limit.getOrElse(1000000),
            additionalFeatures = conf.additionalfeatures.getOrElse(List()),
            limitItemsets = conf.itemsetlimit.getOrElse(1),
            userid = conf.userid.getOrElse("")
        )
    }

    /**
     * @param inTable Input table
     * @param minsize Minimum size of the co-movement patterns
     * @param minsup  Minimum support of the co-movement patterns (i.e., how long trajectories moved together)
     * @param bin_s   bin_s \in N (1, ..., n). Multiply cell size 123m x bin_s
     */
    def run(inTable: String, minsize: Int, minsup: Int, bin_s: Int, timescale: TemporalScale, bin_t: Int, eps_t: Double, eps_s: Double, euclidean: Boolean, querytype: String, limit: Int, limitItemsets: Int, additionalFeatures: List[String], userid: String): Unit = {
        querytype match {
            case "export" => exportData(inTable, minsize, minsup, bin_s, timescale, bin_t, eps_t, euclidean, limit, limitItemsets, additionalFeatures, userid)
            case "user" => exportData2(inTable, minsize, minsup, bin_s, timescale, bin_t, eps_t, euclidean, limit, limitItemsets, additionalFeatures, userid)
            case "stats" => statistics(inTable, minsize, minsup, bin_s, timescale, bin_t, eps_t, eps_s, euclidean, limit, limitItemsets, additionalFeatures, userid)
        }
    }

    /** Export original and binned data from the valid patterns */
    def exportData(inTable: String, minsize: Int, minsup: Int, bin_s: Int, timescale: TemporalScale, bin_t: Int, eps_t: Double, euclidean: Boolean, lmt: Int, limitItemsets: Int, additionalFeatures: List[String], userid: String): Unit = {
        val sparkSession = startSparkSession(appName = "QueryCTM", master = "yarn")
        val epst = if (eps_t == Int.MaxValue || eps_t == Double.PositiveInfinity) "Infinity" else eps_t.toString
        val semf = if (additionalFeatures.isEmpty) "" else s"__semf_" + additionalFeatures.reduce(_ + _)
        val selectTrajectories = if (userid.isEmpty) s"trajectory.${inTable}" else s"(select * from trajectory.${inTable} where userid = '$userid')"
        val selectItemset = if (userid.isEmpty) s"(select itemsetid, support from ctm.CTM__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}${semf}__epss_Infinity__epst_${epst}__freq_1__sthr_1000000__summary order by support desc, size asc limit $limitItemsets) a join" else ""
        val sql = //  order by 2 desc limit 2
            s"""select i.itemsetid, t.itemid, t.tid, t.latitude as bin_latitude, t.longitude as bin_longitude, t.time_bucket as bin_timestamp, u.tileid as in_support, s.userid, s.trajectoryid, s.`timestamp`, s.latitude, s.longitude ${if (additionalFeatures.isEmpty) "" else ", " + additionalFeatures.reduce(_ + ", " + _)}
               |from $selectItemset
               |     ctm.CTM__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}${semf}__epss_Infinity__epst_${epst}__freq_1__sthr_1000000__itemset i ${if (selectItemset.nonEmpty) "on (a.itemsetid = i.itemsetid)" else ""}
               |     join ctm.tmp_transactiontable__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}${semf} t on (t.itemid = i.itemid)
               |     left join ctm.CTM__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}${semf}__epss_Infinity__epst_${epst}__freq_1__sthr_1000000__support u on (t.tid = u.tileid and i.itemsetid = u.itemsetid)
               |     join $selectTrajectories s on (t.userid = s.userid and t.trajectoryid = s.trajectoryid
               |        ${if (additionalFeatures.isEmpty) "" else "and " + additionalFeatures.zipWithIndex.map(i => s"split(t.semf, '-')[${i._2}] = s.${i._1}").reduce(_ + " and " + _) }
               |        and ${computeTimeBin(timescale, bin_t, column = "s.`timestamp`")} = t.time_bucket
               |        and ${computeLatitudeQuery(euclidean, bin_s, "s.latitude")} = t.latitude
               |        and ${computeLongitudeQuery(euclidean, bin_s, "s.longitude")} = t.longitude)""".stripMargin
        var tablename = s"join__${inTable}__${minsize}__${minsup}__${bin_s}__${timescale.value}__${bin_t}${semf}"

        println(s"SQL:\n$sql")
        sparkSession.sql("use ctm")
        sparkSession.sql(sql).write.mode(SaveMode.Overwrite).saveAsTable(tablename)

        val pw = new PrintWriter(new File("saveQuery.sh"))
        var hivequery = s"select itemsetid, itemid, tid, userid, trajectoryid, `timestamp`, latitude, longitude, bin_latitude, bin_longitude, bin_timestamp, in_support ${if (additionalFeatures.isEmpty) "" else ", " + additionalFeatures.reduce(_ + ", " + _)} from $tablename"
        var hiveCommand = s"hive -e 'set hive.cli.print.header=true; use ctm; $hivequery' | sed 's/[\\t]/,/g'  > $tablename.csv"
        println(hiveCommand)
        pw.write(s"$hiveCommand\n")

        // sparkSession.sql(s"select itemid, bin_latitude, bin_longitude, bin_timestamp from $tablename").rdd.map(r => (r.get(3), (r.get(0), r.get(1), r.get(2))))
        hivequery = s"select distinct itemid, bin_latitude, bin_longitude, bin_timestamp from $tablename"
        hiveCommand = s"hive -e 'set hive.cli.print.header=true; use ctm; $hivequery' > $tablename.tsv"
        var hdfsCommand = s"hdfs dfs -put $tablename.tsv /user/mfrancia/spare/input"
        println(hiveCommand)
        pw.write(s"$hiveCommand\n")
        println(hdfsCommand)
        pw.write(s"$hdfsCommand\n")

        tablename = s"tmp_transactiontable__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}${semf}" + (if (additionalFeatures.isEmpty) "" else s"__semf_" + additionalFeatures.reduce(_ + _))

        hivequery = s"select itemid, latitude, longitude, time_bucket from $tablename"
        hiveCommand = s"hive -e 'set hive.cli.print.header=true; use ctm; $hivequery' > $tablename.tsv"
        hdfsCommand = s"hdfs dfs -put $tablename.tsv /user/mfrancia/spare/input"
        println(hiveCommand)
        pw.write(s"$hiveCommand\n")
        println(hdfsCommand)
        pw.write(s"$hdfsCommand\n")

        // df.repartition(1).write.option("sep", ",").option("header", "true").option("escape", "\"").option("quoteAll", "true").csv("file:////home/mfrancia/ctm/output/")
        pw.close()

        execCmd("chmod u+x saveQuery.sh")
        execCmd("./saveQuery.sh")
    }

    def statistics(inTable: String, minsize: Int, minsup: Int, bin_s: Int, timescale: TemporalScale, bin_t: Int, eps_t: Double, eps_s: Double, euclidean: Boolean, lmt: Int, limitItemsets: Int, additionalFeatures: List[String], userid: String = "") = {
        val epst = if (eps_t == Int.MaxValue || eps_t == Double.PositiveInfinity) "Infinity" else eps_t.toInt.toString
        val epss = if (eps_s == Int.MaxValue || eps_s == Double.PositiveInfinity) "Infinity" else eps_s.toInt.toString
        val semf = if (additionalFeatures.isEmpty) "" else s"__semf_" + additionalFeatures.reduce(_ + _)
        val sparkSession = startSparkSession(appName = "QueryCTM", master = "yarn", nexecutors = 10, maxram = "12g")
        println("statistics")
        sparkSession.sql("use ctm")

        def getSQL(f: Boolean) = {
            val sql =
                s"""
                   |   select a.latitude, a.longitude, a.time_bucket, ${if (f) "a.semf, " else ""}count(distinct a.itemid) as c
                   |   from
                   |       (select distinct latitude, longitude, time_bucket, semf, tid, itemid from ctm.tmp_transactiontable__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}${if (f) semf else ""}) a
                   |       join
                   |       (select distinct tileid from ctm.CTM__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}${if (f) semf else ""}__epss_${epss}__epst_${epst}__freq_1__sthr_1000000__support) b on (a.tid = b.tileid)
                   |       join
                   |       (select distinct itemid from ctm.CTM__tbl_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}${if (f) semf else ""}__epss_${epss}__epst_${epst}__freq_1__sthr_1000000__itemset) c on (a.itemid = c.itemid)
                   |   group by a.latitude, a.longitude, a.time_bucket${if (f) ", a.semf" else ""}
            """.stripMargin
            val tableName = s"tmp_stats_$f"
            println("\n")
            println(sql)
            println("\n")
            sparkSession.sql(sql).write.mode(SaveMode.Overwrite).saveAsTable(tableName)
        }

        getSQL(false)
        getSQL(true)

        val sql =
            s"""
               |   select a.latitude, a.longitude, a.time_bucket, a.c, b.c as csemf
               |   from
               |       tmp_stats_false a
               |       full outer join
               |       tmp_stats_true b on (a.latitude = b.latitude and a.longitude = b.longitude and a.time_bucket = b.time_bucket)
            """.stripMargin
        val tableName = s"stats_${inTable}__lmt_${lmt}__size_${minsize}__sup_${minsup}__bins_${bin_s}__ts_${timescale.value}__bint_${bin_t}__epss_${epss}__epst_${epst}"
        println("\n")
        println(sql)
        println("\n")
        sparkSession.sql(sql).write.mode(SaveMode.Overwrite).saveAsTable(tableName)

        val pw = new PrintWriter(new File("saveQuery.sh"))
        val hivequery = s"select latitude, longitude, time_bucket, c, csemf from $tableName"
        val hiveCommand = s"hive -e 'set hive.cli.print.header=true; use ctm; $hivequery' | sed 's/[\\t]/,/g'  > $tableName.csv"
        println(hiveCommand)
        pw.write(s"$hiveCommand\n")
        pw.close()

        execCmd("chmod u+x saveQuery.sh")
        execCmd("./saveQuery.sh")
    }

    /** Export original and binned data from the valid patterns */
    def exportData2(inTable: String, minsize: Int, minsup: Int, bin_s: Int, timescale: TemporalScale, bin_t: Int, eps_t: Double, euclidean: Boolean, lmt: Int, limitItemsets: Int, additionalFeatures: List[String], userid: String = ""): Unit = {
        var id = userid
        if (id.isEmpty) {
            val sparkSession = startSparkSession(appName = "QueryCTM", master = "yarn")
            val sql = s"""select userid, count(*) as c from trajectory.${inTable} group by userid order by c desc limit 1"""
            id = sparkSession.sql(sql).collect()(0).get(0).toString
        }
        exportData(inTable, minsize, minsup, bin_s, timescale, bin_t, eps_t, euclidean, lmt, limitItemsets, additionalFeatures, id)
    }

    @throws[java.io.IOException]
    def execCmd(cmd: String) = {
        val proc = Runtime.getRuntime.exec(cmd)
        proc.waitFor()
        val is = proc.getInputStream
        val s = new Scanner(is).useDelimiter("\\A")
        var `val` = ""
        if (s.hasNext) `val` = s.next
        else `val` = ""
        println(`val`)
    }
}

/**
 * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
 *
 * @param arguments the programs arguments as an array of strings.
 */
//class Conf2(arguments: Seq[String]) extends Conf(arguments) {
class Conf2(arguments: Seq[String]) extends ScallopConf(arguments) {
    val querytype = opt[String]()
    val userid = opt[String]()
    val itemsetlimit = opt[Int]()
    val tbl = opt[String]()
    val limit = opt[Int]()
    val minsize = opt[Int]()
    val minsup = opt[Int]()
    val euclidean = opt[Boolean]()
    val platoon = opt[Boolean]()
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
    val returnresult = opt[Boolean]()
    val additionalfeatures = opt[List[String]]()
    verify()
}