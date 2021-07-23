package euro2020

import scala.io.Source

/**
 * Data from: https://www.kaggle.com/mcarujo/euro-cup-2020
 */
object Euro2020Data {

  private[euro2020] def lines: List[String] = {
    Option(getClass.getResourceAsStream("/euro2020/eurocup_2020_results.csv")) match {
      case None => sys.error("There is no data")
      case Some(resource) => Source.fromInputStream(resource).getLines().toList
    }
  }

  /**
   *
   * @param stage Competition Fase/Stage. Eg: Final, Semi-finals, and etc.
   * @param date When the match happened
   * @param pens If the match ends with penaltis or normal time
   * @param pensHomeScore In case of penaltis, the team home scores
   * @param pensAwayScore In case of penaltis, the team away scores
   * @param teamNameHome The team home name
   * @param teamNameAway The team away name
   * @param teamHomeScore The team home's scores
   * @param teamAwayScore The team away's scores
   */
  case class Euro2020Article(stage: String,
                             date: String,
                             pens: String,
                             pensHomeScore: String,
                             pensAwayScore: String,
                             teamNameHome: String,
                             teamNameAway: String,
                             teamHomeScore: Int,
                             teamAwayScore: Int
                            ) {}


  private[euro2020] def parse(line: String): Euro2020Article = {
    val chunks = line.split(",")
    def getString(idx: Int) = chunks(idx).trim

    Euro2020Article(getString(0),
      getString(1),
      getString(2),
      getString(3),
      getString(4),
      getString(5),
      getString(6),
      chunks(7).toInt,
      chunks(8).toInt
    )
  }
}
