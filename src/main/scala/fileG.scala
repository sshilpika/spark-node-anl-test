package gov.anl.alcf

import java.io.{File, FileOutputStream, PrintWriter}

import util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

case class Config(fileSize: Option[Int] = Some(1000000),master: Option[String] = Some("cluster"))

object fileG{
  val conf = new SparkConf().setAppName("Random Integer Sort File Generator")
  val spark = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(spark)
  import sqlContext.implicits._

	def main(args:Array[String]):Unit = {

    // getting the values for ingestion using scopt
    val appConfig = parseCommandLine(args).getOrElse(Config())
    val size = appConfig.fileSize.getOrElse(10)
    val master = appConfig.master.getOrElse("")

    val mustSort = fileGen(size).persist()

    val customSchema = StructType(Array(
      StructField("_1", IntegerType, true)))

    //store file before sorting
    val mustSortDF = mustSort.toDF()
    mustSortDF.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("/projects/ExaHDF5/sshilpika/forsort.csv")


    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .schema(customSchema)
      .load("/projects/ExaHDF5/sshilpika/forsort.csv")

    println("DF sorted :")
    val DFSort = performance {



      dataFrameSort(df,size)
    }

    println("RDD SORTED ")
    val RDDSort= performance{
      RddSort(mustSort,size)
    }

    //TODO write results to CSV instead of text file
    //val RDDres = List(("time",RDDSort._1),("space",RDDSort._2),("result_length",RDDSort._3)).csvIterator.mkString("\n")

    //write to results file
    val writer = new PrintWriter(new FileOutputStream(new File("Results.txt"), true))
    writer.append(s"RDDTime : ${RDDSort._1}, RDDSpace : ${RDDSort._2}, RDDResult: ${RDDSort._3}, master : ${master}\n"++
      s"DFTime : ${DFSort._1}, DFSpace : ${DFSort._2}, DFResult: ${DFSort._3}, master : ${master}\n\n"
    )
    writer.close()


    stopSpark

  }

  def fileGen(size : Int): RDD[Int] ={
    val one = spark.parallelize(Seq.fill(size)(size),32)
    one.flatMap(x => Seq.fill(x)(Random.nextInt))
  }

  def fileGenMakeRDD(size : Int): RDD[Int] ={
    val one = spark.makeRDD(Seq.fill(size)(size))
    one.flatMap(x => Seq.fill(x)(Random.nextInt))
  }

    def RddSort(mustSort: RDD[Int],size: Int): Long ={
      val result = mustSort.sortBy(c=>c,true)
      result.saveAsTextFile("/projects/ExaHDF5/sshilpika/RDDSortedRes"+size*size)
      result.count()

    }

    def dataFrameSort(df: DataFrame,size: Int): Long ={

      val sorted = df.sort("_1")
      val writeInto = sorted.write.save("/projects/ExaHDF5/sshilpika/DFSortedRes"+size*size)

      sorted.count()


  }
  /*def dataSetSort(ds: Dataset): Array[Int] ={

    //ds.
    //ds.s //sort().write.save("df.txt")
    //ds.take(100).asInstanceOf[Array[Int]]

  }*/

  def parseCommandLine(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("Ingest", "1.0")
      opt[Int]('s', "size") action { (x, c) =>
        c.copy(fileSize = Some(x))
      } text ("fileSize is an Int property")
      opt[String]('m', "master") action { (x, c) =>
        c.copy(master = Some(x))
      } text ("master is a String property")
      help("help") text ("Usage: -s [fileSize]")
    }
    parser.parse(args, Config())
  }

  def stopSpark(): Unit ={
    spark.stop()
  }

}
