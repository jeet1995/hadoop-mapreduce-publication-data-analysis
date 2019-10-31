package test.scala.com.publication.statistics.computation.bucketingByNumOfNodes

import javax.xml.parsers.SAXParserFactory
import main.scala.com.publication.statistics.computation.bucketingByNumOfNodes.BucketingByNumNodesMapper
import org.scalatest.FunSuite

import scala.xml.XML

class BucketingByNumNodesMapperTest extends FunSuite {


  private val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  private val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI



  test("testDetermineAuthorsPerPublication") {

    val bucketingByNumNodesMapper = new BucketingByNumNodesMapper

    val text = "<phdthesis mdate=\"2019-07-25\" key=\"phd/basesearch/Shih17\">\n     <author>Mimosa Networks</author>\n   <author>Chin Shu</author>\n <author>Xu Shin</author>\n <author>Min Wang</author>\n   <title>Algorithms and protocols for next generation WiFi networks.</title>\n        <year>2017</year>\n        <school>Georgia Institute of Technology, Atlanta, GA, USA</school>\n        <ee>http://hdl.handle.net/1853/58204</ee>\n        <ee>https://www.base-search.net/Record/9d30ea9fbee665fc1765cecd4c3d85af7b4282bc5b07a0e6f46ac261e00fb871</ee>\n        <note type=\"source\">base-search.net (ftgeorgiatech:oai:smartech.gatech.edu:1853/58204)</note>\n    </phdthesis>"
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$text</dblp>"""
    val element = XML.withSAXParser(xmlParser).loadString(xmlString)

    val numAuthors = bucketingByNumNodesMapper.determineAuthorsPerPublication(element)

    assert(numAuthors === 4)
  }

  test("testDeterminePublicationType") {


    val bucketingByNumNodesMapper = new BucketingByNumNodesMapper

    val text = "<phdthesis mdate=\"2019-07-25\" key=\"phd/basesearch/Shih17\">\n     <author>Mimosa Networks</author>\n   <author>Chin Shu</author>\n <author>Xu Shin</author>\n <author>Min Wang</author>\n   <title>Algorithms and protocols for next generation WiFi networks.</title>\n        <year>2017</year>\n        <school>Georgia Institute of Technology, Atlanta, GA, USA</school>\n        <ee>http://hdl.handle.net/1853/58204</ee>\n        <ee>https://www.base-search.net/Record/9d30ea9fbee665fc1765cecd4c3d85af7b4282bc5b07a0e6f46ac261e00fb871</ee>\n        <note type=\"source\">base-search.net (ftgeorgiatech:oai:smartech.gatech.edu:1853/58204)</note>\n    </phdthesis>"
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$text</dblp>"""
    val element = XML.withSAXParser(xmlParser).loadString(xmlString)

    val publicationElement = bucketingByNumNodesMapper.determinePublicationType(element)

    assert(publicationElement === "phdthesis")
  }

}
