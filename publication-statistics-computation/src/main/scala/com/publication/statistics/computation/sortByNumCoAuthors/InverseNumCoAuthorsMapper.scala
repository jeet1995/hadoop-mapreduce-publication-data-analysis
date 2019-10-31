package main.scala.com.publication.statistics.computation.sortByNumCoAuthors

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/**
  * This class denotes a utility mapper which inverts the key-value pairs generated @SortByNumCoAuthorsReducer
  * as the keys are sorted by the sort and shuffle step following the map operation.
  * */
class InverseNumCoAuthorsMapper extends Mapper[Text, Text, IntWritable, Text] with LazyLogging {

  override def map(key: Text, value: Text, context: Mapper[Text, Text, IntWritable, Text]#Context): Unit = {

    val intVal = value.toString.toInt
    logger.info("Mapper InverseNumCoAuthorsMapper emitting (key, value) pair : " + "(" + intVal + "," + key.toString + ")")
    context.write(new IntWritable(intVal), key)
  }

}
