package utils

import org.apache.spark.{SparkConf, SparkContext}

object Spark {
  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("MyApp")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  def getSparkContext: SparkContext = sc
}
