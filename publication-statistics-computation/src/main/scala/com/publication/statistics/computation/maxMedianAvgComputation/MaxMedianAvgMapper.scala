package main.scala.com.publication.statistics.computation.maxMedianAvgComputation

import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.collection.mutable.ArrayBuffer
import scala.xml.{Elem, XML}


/**
  * This class denotes the mapper class which performs a calculation for determining the max, median and avg. for a given author as
  * far as the no. of co-authors is concerned.
  * */
class MaxMedianAvgMapper extends Mapper[LongWritable, Text, Text, MaxMedianAvgWritable] with LazyLogging {

  private val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  private val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, MaxMedianAvgWritable]#Context): Unit = {

    val publicationElement = getPublicationXML(value.toString)
    val authors = getAuthors(publicationElement, context.getConfiguration)

    if (authors.nonEmpty) {
      authors.foreach { author =>
        context.write(new Text(author), MaxMedianAvgWritable((authors.size - 1).asInstanceOf[Float], (authors.size - 1).asInstanceOf[Float], 1, (authors.size - 1).asInstanceOf[Long]))
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

  def getAuthors(publicationElement: Elem, configuration: Configuration): ArrayBuffer[String] = {
    val authors = new ArrayBuffer[String]
    (publicationElement \\ "author").foreach { node => authors += node.text }
    authors
  }


}
