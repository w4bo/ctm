package it.unibo.big.comparison

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

object ClusterSimilarityComparison {
  val LEFTPAR = "{"
  val RIGHTPAR = "}"
  val EMPTYCHAR = ""
  val CSV_SEPARATOR = ","
  val DEFAULT_SPLITTER = "splitter"
  val PARAM_SPLITTER = "_"
  val COMPARISON_FOLDER_PATH = "comparison"

  private def loadOnlyItemsetTable(sparkSession: SparkSession, inputTableName:String) = {
    sparkSession.sql(s"select * from ${inputTableName}")
      .rdd
      .map(row => (row.getAs[Int](0), row.getAs[Int](1).toString))
      .groupByKey()
      .map(_._2.toSet)
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .config("spark.executor.instances", 9)
      .config("spark.executor.cores", 3)
      .config("spark.executor.memory", "12g")
      .getOrCreate()
    spark.sql("use trajectory")
    val tableParamArray = conf.input_table_name().split(DEFAULT_SPLITTER)(1).split(PARAM_SPLITTER)
    val m = tableParamArray(1)
    val k = tableParamArray(2)
    val g = tableParamArray(3)
    val radius = tableParamArray(4)
    val trajectoriesNumber = tableParamArray(5)

    val fileTimeName: String = s"logs/time_" + conf.input_table_name()
    val cuteTime = new BufferedReader(new FileReader(fileTimeName)).readLine().toLong

    val fileGCMPTimeName: String = s"logs/time_" + conf.input_tsv_path().replace("/", "_")
    val gcmpTime = new BufferedReader(new FileReader(fileGCMPTimeName)).readLine().toLong

    val tsvParmArray = conf.input_tsv_path().split(DEFAULT_SPLITTER)(1).split(PARAM_SPLITTER).map(s => s.replace("/",""))
    val epsilon = tsvParmArray(0)
    val minPoints = tsvParmArray(1)

    val sc = spark.sparkContext
    val hadoopconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopconf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(conf.input_tsv_path()))
    val csvDatasetResult = if(exists) {
      sc.textFile(conf.input_tsv_path())
        .map(cluster => cluster
          .replace(LEFTPAR, EMPTYCHAR)
          .replace(RIGHTPAR, EMPTYCHAR)
          .split(CSV_SEPARATOR)
          .map(_.replace(" ", "").replace("\t", ""))
          .toSet)
          .collect()
    } else { Array[Set[String]]() }
    val tableExists = spark.catalog.tableExists(conf.input_table_name())
    val tableDatasetResult = if(tableExists) {
        loadOnlyItemsetTable(spark, conf.input_table_name()).collect()
    } else {
      Array[Set[String]]()
    }
    println("Table clusters")
    tableDatasetResult.foreach(println)
    println("TSV clusters")
    csvDatasetResult.foreach(println)
    val jaccardValue = JaccardComparator
      .computeJaccardOnlyOnItemsetElements(csvDatasetResult,
        tableDatasetResult)
    val tableExclusiveItemsetsCount = JaccardComparator.getExclusiveSetElements(tableDatasetResult, csvDatasetResult).length
    val tsvEsclusiveItemsetsCount = JaccardComparator.getExclusiveSetElements(csvDatasetResult, tableDatasetResult).length
    val marriageSimilarity = MarriageSimilarity.computeMarriageSimilarity(tableDatasetResult, csvDatasetResult)._2
    val filteredMarriageSimilarity = MarriageSimilarity.computeMarriageSimilarity(tableDatasetResult, csvDatasetResult, true)._2
    val closedDatasetPercentage = JaccardComparator.computeSubPattern(tableDatasetResult, csvDatasetResult)

    val fixedTSVName = conf.input_tsv_path().replace("/", "_")
    val fixedTableName = conf.input_table_name()
    val id = s"Cute_GCMP_similarity.csv"
    val outTable = s"$COMPARISON_FOLDER_PATH/$id"
    val fileExists = Files.exists(Paths.get(outTable))
    val outputFile = new File(outTable)
    if(!fileExists) {
      outputFile.createNewFile()
    }
    val bw = new BufferedWriter(new FileWriter(outTable, fileExists))
    if(!fileExists) {
      bw.write("tablename,tsvname,tabletotal,tableexclusive,tsvtotal,tsvexclusive,jaccardsimilarity, marriagesimilarity,filteredmarriagesimilarity,subclusterpercentage,m,k,g,radius,epsilon,minpoints,trajectoriesnumber,cutetime,gcmptime\n")
    }
    bw.write(s"$fixedTableName,$fixedTSVName,${tableDatasetResult.length},${tableExclusiveItemsetsCount},${csvDatasetResult.length},${tsvEsclusiveItemsetsCount},${jaccardValue},${marriageSimilarity},${filteredMarriageSimilarity},$closedDatasetPercentage,$m,$k,$g,$radius,$epsilon,$minPoints,$trajectoriesNumber,$cuteTime,$gcmpTime\n")
    bw.close()
  }
  }


/**
 * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
 *
 * @param arguments the programs arguments as an array of strings.
 */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input_table_name = opt[String](required = true)
  val input_tsv_path = opt[String](required = true)
  verify()
}
