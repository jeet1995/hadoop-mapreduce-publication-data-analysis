package main.scala.com.publication.statistics.computation.sortByNumCoAuthors

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer


/**
  * This class is a reducer which simply emits a sorted key-value pair based on key from InverseNumCoAuthorsMapper.
  * */
class InverseNumCoAuthorsReducer extends Reducer[IntWritable, Text, Text, IntWritable] {

  override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
    values.forEach { value =>
      context.write(value, key)
    }
  }
}
