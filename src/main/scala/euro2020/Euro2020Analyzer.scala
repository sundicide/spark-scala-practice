package euro2020

import euro2020.Euro2020Data.Euro2020Article
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.SparkUtil

object Euro2020Analyzer {


  val sc: SparkContext = SparkUtil.getSparkContext

  // except line 1. bc of line 1 contains cell name
  val euroRdd: RDD[Euro2020Article] = sc.parallelize(Euro2020Data.lines.tail.map(line => Euro2020Data.parse(line)))

  def groupByStage(articles: RDD[Euro2020Article]): RDD[((String, Iterable[Euro2020Article]), Long)] = {
    articles
      .map(article => (article.stage, article))
      .groupByKey()
      .zipWithIndex()
  }

  def groupByTeam(): List[(String, Int)] = {
    euroRdd.map(article => (article.teamNameHome, article.teamNameAway))
      .flatMap(data => List(data._1, data._2))
      .collect()
      .groupBy(d => d)
      .mapValues(arrStr => arrStr.length)
      .toList
      .sortBy(d => d._2)(Ordering.Int)
      .reverse
  }

  def occurrencesOfTeams(teamName: String): Int =
    euroRdd
      .aggregate(0)((sum: Int, a: Euro2020Article) =>
        if ((a.teamNameAway == teamName) || (a.teamNameHome == teamName)) sum + 1 else sum, (b, a) => b + a)

  def main(args: Array[String]): Unit = {
    euroRdd.collect().foreach(println)

    groupByStage(euroRdd).collect().foreach(println)

    println(occurrencesOfTeams("Denmark"))
    println(groupByTeam().mkString("Array(", ", ", ")"))
  }
}
