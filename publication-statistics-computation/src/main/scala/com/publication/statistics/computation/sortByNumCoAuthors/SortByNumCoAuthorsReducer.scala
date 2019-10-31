package main.scala.com.publication.statistics.computation.sortByNumCoAuthors

import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.mutable

/**
  * This class is a reducer class which reduces a author key based on the no. of authors he/ she has worked with
  * across all publications.
  * */
class SortByNumCoAuthorsReducer extends Reducer[Text, Text, Text, IntWritable] with LazyLogging {

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
    val set = new mutable.HashSet[String]()

    values.forEach { value =>
      set += value.toString
    }
    logger.info("Reducer SortByNumCoAuthorsReducer emitting (key, value) pair : " + "(" + key.toString + "," + set.size + ")")
    context.write(key, new IntWritable(set.size))
  }
}
