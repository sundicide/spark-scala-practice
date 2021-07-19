package utils

import scala.io.Source

object FileUtil {
  def getResourcePath: String = {
    val txtFile = Source.fromFile("src/main/resources/config.txt")
    txtFile.getLines().take(1).toList.head
  }
}
