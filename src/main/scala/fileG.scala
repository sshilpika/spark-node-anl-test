package gov.anl.alcf

import java.io.{File, FileOutputStream, PrintWriter}

import util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

case class Config(partitions: Option[Int] = Some(4),fileSize: Option[Int] = Some(1000000),master: Option[String] = Some("cluster"))

object fileG{
  val conf = new SparkConf().setAppName("Random Integer Sort File Generator")
  val spark = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(spark)
  import sqlContext.implicits._
  val customSchema = StructType(Array(
    StructField("_1", IntegerType, true)))

  //var defaultPartitions = 4

	def main(args:Array[String]):Unit = {

    // getting the values for ingestion using scopt
    val appConfig = parseCommandLine(args).getOrElse(Config())
    val size = appConfig.fileSize.getOrElse(10)
    val master = appConfig.master.getOrElse("")
    val  defaultPartitions = appConfig.partitions.getOrElse(4)

    val mustSort = fileGen(size,defaultPartitions).persist()

    //store file before sorting
    val mustSortDF = mustSort.toDF()

    writeCSV(mustSortDF)
    val df = readCSV()

    println("DF sorted :")
    val DFSort = performance {

      dataFrameSort(df,size)
    }

    val ds = mustSort.toDS()
    println("DS sorted :")
    val DSSort = performance {
      dataSetSort(ds)
    }


    println("RDD SORTED ")
    val RDDSort= performance{
      RddSort(mustSort,size)
    }

    //write to results file
    val writer = new PrintWriter(new FileOutputStream(new File("Results.txt"), true))
    writer.append(s"RDDTime : ${RDDSort._1}, RDDSpace : ${RDDSort._2}, RDDResult: ${RDDSort._3}, master : ${master}\n"++
      s"DFTime : ${DFSort._1}, DFSpace : ${DFSort._2}, DFResult: ${DFSort._3}, master : ${master}\n\n"++
      s"DFTime : ${DSSort._1}, DFSpace : ${DSSort._2}, DFResult: ${DSSort._3}, master : ${master}\n\n"
    )
    writer.close()


    stopSpark

  }

  def fileGen(size : Int,defaultPartitions:Int): RDD[Int] ={
    val one = spark.parallelize(Seq.fill(size)(size),defaultPartitions)
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
  def dataSetSort(ds: Dataset[Int]): Dataset[Int] ={

    //quickSort(ds)
    val z = ds.mapPartitions(items => {
      quickSort(items.toArray[Int]).iterator
    })

    //merge sort for partitions
    (1 until 4).foldLeft(z:Dataset[Int]){case(dsTmp,x)=> {
      dsTmp.coalesce(4-x).mapPartitions(items => {
        mergeSort(items.toList).toIterator
      })

    }}

  }

  def writeCSV(mustSortDF: DataFrame): Unit ={
    mustSortDF.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("/projects/ExaHDF5/sshilpika/forsort.csv")
  }

  def readCSV():DataFrame={
    sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .schema(customSchema)
    .load("/projects/ExaHDF5/sshilpika/forsort.csv")
  }

  def parseCommandLine(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("Ingest", "1.0")
      opt[Int]('s', "size") action { (x, c) =>
        c.copy(fileSize = Some(x))
      } text ("fileSize is an Int property")
      opt[Int]('p', "partitions") action { (x, c) =>
        c.copy(partitions = Some(x))
      } text ("partition is an Int property")
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
