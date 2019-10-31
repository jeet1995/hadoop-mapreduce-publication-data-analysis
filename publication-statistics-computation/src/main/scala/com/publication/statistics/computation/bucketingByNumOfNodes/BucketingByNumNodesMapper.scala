package main.scala.com.publication.statistics.computation.bucketingByNumOfNodes

import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory
import main.scala.com.publication.statistics.computation.utils.ApplicationConstants
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.xml.{Elem, XML}


/**
  * This class denotes the mapper class which performs a bucketing based on some node in the XML. The XML
  * is basically the format of the data in which publication information is represented.
  **/

class BucketingByNumNodesMapper extends Mapper[LongWritable, Text, Text, IntWritable] with LazyLogging {

  private val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  private val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  private var jobName = ""
  private var bucketSize = 3

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val publicationData = getPublicationXMLFromPublicationText(value.toString)

    jobName match {
      case ApplicationConstants.BUCKETING_BY_NUM_CO_AUTHOR =>
        val authorCount = determineAuthorsPerPublication(publicationData)
        val bucket = KeyGenerator.generateKeyByBucket(authorCount, bucketSize)

        logger.info("Mapper BucketingByNumNodesMapper emitting (key, value) pair : " + "(" + bucket + "," + 1.toString + ")")

        context.write(new Text(bucket), new IntWritable(1))
      case ApplicationConstants.BUCKETING_BY_PUBLICATION_TYPE =>
        val publicationType = determinePublicationType(publicationData)

        logger.info("Mapper BucketingByNumNodesMapper emitting (key, value) pair : " + "(" + publicationType + "," + 1.toString + ")")
        context.write(new Text(publicationType), new IntWritable(1))
    }
  }

  /**
    * This is an overidden method which allows the mapper to be configurable based on job name and bucket size
    **/
  override def setup(context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    jobName = context.getConfiguration.get(ApplicationConstants.JOB_NAME)
    bucketSize = context.getConfiguration.getInt(ApplicationConstants.BUCKET_SIZE, 3)
  }


  /**
    * @param publicationText This input represents a publication element in plain text.
    * @return This method returns publication text as an XML element.
    **/
  private def getPublicationXMLFromPublicationText(publicationText: String): Elem = {
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$publicationText</dblp>"""

    XML
      .withSAXParser(xmlParser)
      .loadString(xmlString)
  }


  /**
    * @param publicationData The publication element is taken as an input.
    * @return This method returns the no. of authors per publication.
    **/
  def determineAuthorsPerPublication(publicationData: Elem): Int = {

    var author = ""

    publicationData.child.head.label match {

      case "book" =>
        author = "editor"
      case "proceedings" =>
        author = "editor"
      case _ =>
        author = "author"
    }
    (publicationData \\ author).size
  }

  /**
    * @param publicationData The publication element is taken as an input
    * @return This method returns the publication type which is basically the label representing the
    *         second node in the publication XML hierarchy.
    **/
  def determinePublicationType(publicationData: Elem): String = publicationData.child.head.label


}
