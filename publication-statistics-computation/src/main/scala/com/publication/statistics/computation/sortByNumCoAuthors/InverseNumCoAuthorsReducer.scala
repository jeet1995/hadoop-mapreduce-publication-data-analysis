package main.scala.com.publication.statistics.computation.sortByNumCoAuthors

import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer


/**
  * This class is a reducer which simply emits a sorted key-value pair based on key from InverseNumCoAuthorsMapper.
  * */
class InverseNumCoAuthorsReducer extends Reducer[IntWritable, Text, Text, IntWritable] with LazyLogging {

  override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
    values.forEach { value =>
      logger.info("Reducer InverseNumCoAuthorsReducer emitting (key, value) pair : " + "(" + value.toString + "," + key.get + ")")
      context.write(value, key)
    }
  }
}
