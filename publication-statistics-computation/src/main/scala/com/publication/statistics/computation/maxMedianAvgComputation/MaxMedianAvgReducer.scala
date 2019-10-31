package main.scala.com.publication.statistics.computation.maxMedianAvgComputation

import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.mutable

/**
  * This class denotes a reducer class which computes max, median and average no, of co-authors and author has published with across papers.
  **/
class MaxMedianAvgReducer extends Reducer[Text, MaxMedianAvgWritable, Text, Text] with LazyLogging {

  override def reduce(key: Text, values: lang.Iterable[MaxMedianAvgWritable], context: Reducer[Text, MaxMedianAvgWritable, Text, Text]#Context): Unit = {

    var sum = 0f
    var count = 0l
    var max = 0f
    var counts = new mutable.ArrayBuffer[Long]()

    values.forEach { value =>
      // compute total by summing avg per paper which is essentially the number of co-authors for 1 paper
      sum += value.getAvg

      // aggregate all counts
      count += value.getCount

      // compute max
      max = if (value.getMax > max) value.getMax else max

      // compute median
      counts += value.getMedian
    }

    // sort all counts
    counts = counts.sorted
    logger.info("Reducer MaxMedianAvgReducer emitting (key, value) pair : " + "(" + key.toString + "," + "(" + max.toString + "," + counts((counts.length - 1) / 2).toString + "," + (sum / count).toString + ")")
    context.write(key, new Text(";" + max.toString + ";" + counts((counts.length - 1) / 2) + ";" + (sum / count).toString))
  }

}
