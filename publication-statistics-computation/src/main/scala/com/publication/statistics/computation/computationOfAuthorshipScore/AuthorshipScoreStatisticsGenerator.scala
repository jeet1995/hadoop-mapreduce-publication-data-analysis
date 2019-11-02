package main.scala.com.publication.statistics.computation.computationOfAuthorshipScore

import scala.collection.mutable
import scala.xml.Elem

object AuthorshipScoreStatisticsGenerator {


  /**
    * @param publicationElement which is the publication element.
    * @return This method returns a map which contains the author name as the key and
    *         authorship score as the value. It is calculated by first assigning a score of 1/N to each
    *         author for a given publication with N authors. Then it iteratively takes away (1/4)th of the score
    *         from the kth author an adds it to the (k - 1)th author. This goes on until we reach the 1st author
    *         of the paper.
    * */
  def getAuthorScoreMap(publicationElement: Elem): mutable.HashMap[String, Float] = {

    val ONE_BY_FOUR = 0.25f
    val ONE = 1f
    val authorStr = "author"

    var author = ""

    publicationElement.child.head.label match {

      case "book" =>
        author = "editor"
      case "proceedings" =>
        author = "editor"
      case _ =>
        author = "author"
    }

    (publicationElement \\ author).size


    val mapSize = (publicationElement \\ authorStr).size
    val authorScoreMap = new scala.collection.mutable.HashMap[String, Float]
    val authors = new mutable.ArrayBuffer[String]

    if (mapSize > 0) {

      // Assign 1/N score to each author
      (publicationElement \\ authorStr).foreach { node =>
        val size = authorScoreMap.size
        if (node.text != null) {
          authors += node.text
          authorScoreMap.put(node.text, ONE / mapSize.asInstanceOf[Float])
        }
      }

      var size = mapSize - 1

      // Execute the aforementioned algorithm iteratively
      authors.reverse.foreach { author =>
        if (size > 0) {
          val differential = ONE_BY_FOUR * authorScoreMap(author)
          authorScoreMap.put(author, authorScoreMap(author) - differential)
          authorScoreMap.put(authors(size - 1), authorScoreMap(authors(size - 1)) + differential)
          size -= 1
        }
      }
    }
    authorScoreMap
  }

}
