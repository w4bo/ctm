package it.unibo.big.preprocessing

import it.unibo.big.Utils
import org.apache.spark.sql.Row
import org.rogach.scallop.ScallopConf

/**
 * This class will convert a valid TSV dataset to a valid table for cute.
 */
object TSVDatasetConverter {

  private def mapFunction(row:Array[String], multiplicativeFactor:Long, artificial:Boolean): Row ={
    if(artificial){
      Row(row(10), row(10), row(5).toDouble, row(6).toDouble, row(2).toLong * multiplicativeFactor)
    } else {
      Row(row(0), row(0), row(1).toDouble, row(2).toDouble, row(3).toLong * multiplicativeFactor)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark = createSparkSessionAndSetTrajectory()
    val multiplicativeFactor = conf.sampling_ratio()
    val isArtificial = conf.artificial()
    val sc = spark.sparkContext
    val firstLine = sc.textFile(conf.input_file_path()).cache().first()
    val gcmpRDD = sc.textFile(conf.input_file_path())
      .filter(line => !(line equals firstLine) || isArtificial)
      .map(_.split(Utils.FIELD_SEPARATOR))
      .map(mapFunction(_, multiplicativeFactor, isArtificial))
    val output = conf.output_path()
    if (conf.writeonhive()) {
      spark.sql("use trajectory")
      spark.sql(s"drop table if exists $output")
      spark.createDataFrame(gcmpRDD, Utils.INPUT_REQUIRED_SCHEMA).write.mode("overwrite").saveAsTable(output)
    } else {
      spark.createDataFrame(gcmpRDD, Utils.INPUT_REQUIRED_SCHEMA).write.mode("overwrite").option("sep", "\t")
        .option("encoding", "UTF-8").csv(output)
    }
  }

  /**
   * Class to be used to parse CLI commands, the values declared inside specify name and type of the arguments to parse.
   *
   * @param arguments the programs arguments as an array of strings.
   */
  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val sampling_ratio = opt[Long](required = true)
    val input_file_path = opt[String](required = true)
    val output_path = opt[String](required = true)
    val writeonhive = opt[Boolean]()
    val artificial = opt[Boolean]()
    verify()
  }

}


