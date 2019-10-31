package main.scala.com.publication.statistics.computation.maxMedianAvgComputation

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable


/**
  * This class is a custom @Writable class which encapsulates the max, avg, count of co-authors and the median of co-authors for a given author.
  * */
case class MaxMedianAvgWritable(var max: Float, var avg: Float, var count: Long, var median: Long) extends Writable {

  def this() = this(0f, 0f, 0l, 0l)

  override def write(out: DataOutput): Unit = {
    out.writeFloat(max)
    out.writeFloat(avg)
    out.writeLong(count)
    out.writeLong(median)
  }

  override def readFields(in: DataInput): Unit = {
    max = in.readFloat()
    avg = in.readFloat()
    count = in.readLong()
    median = in.readLong()
  }

  def getMax: Float = max

  def getAvg: Float = avg

  def getCount: Long = count

  def getMedian: Long = median

}

