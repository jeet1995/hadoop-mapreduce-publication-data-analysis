package main.scala.com.publication.statistics.computation.computationOfAuthorshipScore

import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io.{FloatWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.xml.{Elem, XML}


/**
  * This class denotes the mapper class which performs a calculation for the authorship score based on some node in the XML. The XML
  * is basically the format of the data in which publication information is represented.
  * */
class AuthorshipScoreMapper extends Mapper[LongWritable, Text, Text, FloatWritable] {

  private val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  private val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI


  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, FloatWritable]#Context): Unit = {

    val publicationXML = getXMLElementFromXMLString(value.toString)
    val authorScoresMap = AuthorshipScoreStatisticsGenerator.getAuthorScoreMap(publicationXML)

    if (authorScoresMap.nonEmpty) {
      authorScoresMap.foreach { authorScoreMap =>
        context.write(new Text(authorScoreMap._1), new FloatWritable(authorScoreMap._2))
      }
    }
  }

  def getXMLElementFromXMLString(publicationString: String): Elem = {

    val xmlStringToParse =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$publicationString</dblp>"""

    XML.withSAXParser(xmlParser).loadString(xmlStringToParse)

  }

}
