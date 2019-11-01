package main.scala.com.publication.statistics.computation.sortByNumCoAuthors

import org.apache.hadoop.io.{IntWritable, WritableComparable, WritableComparator}

/**
  * This class denotes a subtype of the WritableComparator class which sorts integer valued keys in descending order.
  * */
class DescendingOrderByNumCoAuthorComparator extends WritableComparator(classOf[IntWritable], true) {

  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    val key1 = a.asInstanceOf[IntWritable]
    val key2 = b.asInstanceOf[IntWritable]
    -1 * key1.compareTo(key2) // Sorts in descending order
  }
}
