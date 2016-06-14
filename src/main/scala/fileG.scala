import org.apache.spark.rdd.RDD
import util.Random
import org.apache.spark._
object fileG{
    val conf = new SparkConf().setAppName("Random Integer Sort File Generator")
    val spark = new SparkContext(conf)

	def main(args:Array[String]):Unit = {

      val mustSort = fileGen
      println(RddSort(mustSort))


  }
    def fileGen(): RDD[Int] ={
        val one = spark.parallelize(Seq.fill(10)(10))
        one.flatMap(x => Seq.fill(x)(Random.nextInt))
    }

    def RddSort(mustSort: RDD[Int]): Array[Int] ={
        val result = mustSort.sortBy(c=>c,true)
        result.saveAsTextFile("RDDSortedRes")
        result.take(100)

    }

}
