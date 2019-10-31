package main.scala.com.publication.statistics.computation.sortByNumCoAuthors

import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.collection.mutable.ArrayBuffer
import scala.xml.{Elem, XML}


/**
  * This class denotes a mapper which sorts authors based on the no. of unique co-authors the author has worked with
  * across publications.
  * */
class SortByNumCoAuthorsMapper extends Mapper[LongWritable, Text, Text, Text] {

  private val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  private val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {

    val publicationXML = getPublicationXML(value.toString)
    val authors = getCoAuthors(publicationXML)

    // map an author to a co-author
    if (authors.nonEmpty) {
      authors.foreach { author =>
        authors.foreach {
          authorRecursive =>
            if (author != authorRecursive)
              context.write(new Text(author), new Text(authorRecursive))
        }
      }
    }
  }


  def getPublicationXML(publicationText: String): Elem = {
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$publicationText</dblp>"""

    XML
      .withSAXParser(xmlParser)
      .loadString(xmlString)

  }

  /**
    * @param publicationElement The publication element.
    * @return This method returns the list of co-authors for the publication.
    * */
  def getCoAuthors(publicationElement: Elem): ArrayBuffer[String] = {
    val authors = new ArrayBuffer[String]()

    var author = ""

    publicationElement.child.head.label match {

      case "book" =>
        author = "editor"
      case "proceedings" =>
        author = "editor"
      case _ =>
        author = "author"
    }

    (publicationElement \\ author).foreach { node =>
      if (node.text != null) {
        authors += node.text
      }
    }
    authors
  }

}
