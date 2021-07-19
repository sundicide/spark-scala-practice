import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.Assert.assertTrue
import org.junit.Test
import utils.SparkUtil

class ScalaTestSuite {

  @Test def `toInt Test`(): Unit  = {
    println("1".toInt)
    println("0".toInt)
  }

}
