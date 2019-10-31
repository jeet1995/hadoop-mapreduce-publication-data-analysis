package main.scala.com.publication.statistics.computation.bucketingByNumOfNodes

import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._


/**
  * This class is a reducer class which reduces a bucket key based on the total no. of XML nodes belonging to that
  * bucket.
  * */

class BucketingByNumNodesReducer extends Reducer[Text, IntWritable, Text, IntWritable] with LazyLogging {

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val value = values.asScala.foldLeft(new IntWritable(0)) { (a, b) => new IntWritable(a.get + b.get) }
    context.write(key, value)

  }
}
