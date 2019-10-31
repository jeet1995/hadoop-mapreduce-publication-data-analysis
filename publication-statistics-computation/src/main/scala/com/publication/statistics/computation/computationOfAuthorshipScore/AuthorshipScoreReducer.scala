package main.scala.com.publication.statistics.computation.computationOfAuthorshipScore

import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.{FloatWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._


/**
  * This class is a reducer class which reduces a author key based on the authorship score the author has received
  * across all his/ her publications.
  * */
class AuthorshipScoreReducer extends Reducer[Text, FloatWritable, Text, FloatWritable] with LazyLogging {

  override def reduce(key: Text, values: lang.Iterable[FloatWritable], context: Reducer[Text, FloatWritable, Text, FloatWritable]#Context): Unit = {

    val value = values.asScala.foldLeft(new FloatWritable(0f)) { (a, b) => new FloatWritable(a.get() + b.get()) }
    context.write(key, value)
  }
}
