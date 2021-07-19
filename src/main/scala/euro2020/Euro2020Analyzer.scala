package euro2020

import euro2020.Euro2020Data.Euro2020Article
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.SparkUtil

object Euro2020Analyzer {


  val sc: SparkContext = SparkUtil.getSparkContext

  // except line 1. bc of line 1 contains cell name
  val euroRdd: RDD[Euro2020Article] = sc.parallelize(Euro2020Data.lines.tail.map(line => Euro2020Data.parse(line)))

  def main(args: Array[String]): Unit = {
    euroRdd.collect().foreach(println)
  }
}
