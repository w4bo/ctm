package it.unibo.big

import java.util.{Calendar, GregorianCalendar, Locale, TimeZone}

import it.unibo.big.Utils.MyMath._
import it.unibo.big.Utils._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random


/*
spark-submit --driver-java-options "-Dhive.metastore.uris=thrift://isi-bigcluster1.csr.unibo.it:9083" --files=/etc/hive/conf/hive-site.xml \
             --num-executors 20 --executor-memory 10G --master yarn --deploy-mode client --executor-cores 6 --class "it.unibo.big.Enrich" BIG-trajectory-all.jar \
             foo ddt_mdc_gps_records_enr 3600 1000 0 "select userid, cast(time as double) as time, latitude, longitude, cast(-1 as double) as accuracy from mdc_gps_records"

spark-submit --driver-java-options "-Dhive.metastore.uris=thrift://isi-bigcluster1.csr.unibo.it:9083" --files=/etc/hive/conf/hive-site.xml \
             --num-executors 20 --executor-memory 10G --master yarn --deploy-mode client --executor-cores 6 --class "it.unibo.big.Enrich" BIG-trajectory-all.jar \
             foo ddt_mdc_gps_wlan_records_enr 3600 1000 0 "select userid, cast(time as double) as time, latitude, longitude, cast(-1 as double) as accuracy from mdc_gps_wlan_records"
*/
object Enrich {
  def main(args: Array[String]): Unit = {
    val inTable: String = args(0).toString // "cariploext"
    val outTable: String = args(1).toString // "cariploenr5"
    val maxTimeDiff: Int = args(2).toInt // 3600 seconds to build a trajectory
    val maxSpaceDiff: Int = args(3).toInt // 1000 meters
    val minPoints: Int = args(4).toInt // 200

    val defaultSql = "select customid, timestamp, latitude, longitude, accuracy" +
      " from " + inTable +
      " where customid is not null and timestamp is not null and longitude is not null and latitude is not null and accuracy is not null" +
      " and timestamp between 1504224000 and 1514764800" +
      " and accuracy < 100"
    val sql: String = if (args.length > 5) args(5) else defaultSql

    val round = 6

    val spark: SparkSession = SparkSession.builder().appName("Cariplo").enableHiveSupport.getOrCreate
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    //noinspection ScalaStyle
    import spark.implicits._

    val hiveContext = spark.sqlContext
    hiveContext.sql("use trajectory")
    println(sql)
    println("write to: " + outTable)
    hiveContext.sql(sql).rdd
      .map(x => (x.getString(0), (x.getString(0), x.getDouble(1).toInt, x.getDouble(2), x.getDouble(3), x.getDouble(4).toInt)))
      .groupByKey
      .filter(_._2.size > minPoints) // consider only users with a sufficient number of points
      .flatMapValues(it => {
        val r = new Random(0)
        var trjid = 0
        var prev: (String, Int, Double, Double, Int) = ("foo", -Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE) // se mantenessi la lista di N punti precedenti, potrei fare una finestra per validare il punto corrente sulla base di quelli passati
        it.groupBy({ case (_: String, timest: Int, _: Double, _: Double, _: Int) => timest / 60 }) // group by time to remove duplicates (non togliere del tutto questo group by, potresti avere più punti nello stesso secondo, ti basta fare group by su timsest)
          .mapValues(l => l.minBy(_._5)) // choose the duplicate with the best accuracy
          .values.toSeq // get the values from the new map
          .sortBy(_._2) // sort them by time
          .map({ case (cid: String, timest: Int, lat: Double, lon: Double, acc: Int) => // qui mappo ogni punto, in cascata potrei aggiungere una filter per filtrare gli outlier
            var dtime: Int = timest - prev._2 // seconds
            var dspace: Double = Distances.haversine(lat, lon, prev._3, prev._4) * 1000 // km to meters
            var speed: Double = dspace / dtime
            if (dtime > maxTimeDiff || dspace > maxSpaceDiff) {
              // Quando devo assegnare un nuova traiettoria?
              // 1. Aggiungere il controllo su variazione velocità, potrei considerare e dividere traiettorie da fermo / in movimento
              dtime = 0
              dspace = 0
              speed = 0
              trjid += 1
            }
            prev = (cid, timest, lat, lon, acc)
            val c = new GregorianCalendar(TimeZone.getTimeZone("Europe/Rome"), Locale.ITALY)
            c.setTime(new java.util.Date(timest * 1000L))
            (cid, timest, roundAt(lat, round), roundAt(lon, round), acc, dtime, roundAt(dspace, round), roundAt(speed, round),
              trjid, roundAt(r.nextDouble, round), c.get(Calendar.DAY_OF_WEEK), c.get(Calendar.HOUR_OF_DAY), GeoJSON.toPointGeoJSON(lon, lat, round))
          })
      })
      .values
      .toDF("userid", "timest", "latitude", "longitude", "accuracy", "timediff", "spacediff", "speed", "trajid", "rnd", "weekday", "dayhour", "geojson")
      .write.mode(SaveMode.Append)
      .saveAsTable(outTable)
    spark.close
  }
}
