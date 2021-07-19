import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.Assert.assertTrue
import org.junit.Test
import utils.Spark

class RDDTestSuite {

  val sc: SparkContext = Spark.getSparkContext

  val as = List(
    (101, ("Ruetli", "AG")),
    (102, ("Brelaz", "DemiTarif")),
    (103, ("Gress", "DemiTarifVisa")),
    (104, ("Schatten", "DemiTarif")))
  val abos: RDD[(Int, (String, String))] = sc.parallelize(as)

  val ls = List(
    (101, "Bern"),
    (101, "Thun"),
    (102, "Lausanne"),
    (102, "Geneve"),
    (102, "Nyon"),
    (103, "Zurich"),
    (103, "St-Gallen"),
    (103, "Chur")
  )
  val locations: RDD[(Int, String)] = sc.parallelize(ls)

  @Test def `map test`(): Unit  = {
    val numbers = sc.parallelize(10 to 50 by 10)
    numbers.foreach(println)
    // 10
    // 20
    // 30
    // 40
    // 50

    val numbersSquared = numbers.map(Math.pow(_, 2).toInt)
    numbersSquared.foreach(println)
    // 100
    // 400
    // 900
    // 1600
    // 2500

    val reversed = numbersSquared.map(_.toString.reverse) // _는 scala에서 placeholder라고 불린다.
    reversed.foreach(println)
    // 001
    // 004
    // 009
    // 0061
    // 0052

    val topFour = reversed.top(4) // 원소 중 top4를 뽑는다. 순서는 보장하지 않는다.
    topFour.foreach(println)
    // 009
    // 004
    // 0061
    // 0052
  }

  @Test def `aggregate test`(): Unit  = {
    /**
     * aggregate는 reduce를 하면서 결과 타입을 변경하고 싶을 때 사용할 수 있다.
     * 또한 Zero Value를 설정할 수 있다.
     */
    val result: Int = abos.aggregate(0)(
      (sum: Int, curr: (Int, (String, String))) => {
        if (curr._1 == 101) sum + 1
        else sum
      },
      (before, after) => before + after
    )
    assertTrue(result == 1)
  }

  @Test def `join test`(): Unit  = {
    val trackedCustomers = abos.join(locations)
    trackedCustomers.collect().foreach(println)
  }

}
