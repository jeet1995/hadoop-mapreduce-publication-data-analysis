package main.scala.com.publication.statistics.computation.schema

import java.nio.charset.StandardCharsets
import com.google.common.io.Closeables
import com.typesafe.scalalogging.LazyLogging
import main.scala.com.publication.statistics.computation.utils.ApplicationConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

/**
  * This class represents an input format class which is a subtype of the text input format class.
  *
  * Credits: @link{https://github.com/mayankrastogi/faculty-collaboration}
  **/
class DBLPXmlInputFormat extends TextInputFormat with LazyLogging {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    new MultiTagXmlRecordReader(split.asInstanceOf[FileSplit], context.getConfiguration)
  }

  class MultiTagXmlRecordReader(split: FileSplit, conf: Configuration)
    extends RecordReader[LongWritable, Text] {

    // Extracts all possible start tags from the configuration object
    private val possibleStartTags = conf.getStrings(ApplicationConstants.POSSIBLE_START_TAGS).map(_.getBytes(StandardCharsets.UTF_8))

    // Extracts all possible end tags from the configuration object
    private val possibleEndTags = conf.getStrings(ApplicationConstants.POSSIBLE_END_TAGS).map(_.getBytes(StandardCharsets.UTF_8))

    // Start of input split
    private val start = split.getStart

    // End of input split
    private val end = start + split.getLength

    // Input stream to the input split
    private val fsDataInputStream = split.getPath.getFileSystem(conf).open(split.getPath)

    // Output buffer
    private val dataOutputBuffer = new DataOutputBuffer()

    // Key to be written to a mapper
    private var currentKey = new LongWritable()

    // Value to be written to a mapper
    private var currentValue = new Text()


    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      fsDataInputStream.seek(start)
    }

    override def nextKeyValue(): Boolean = {

      // Search for start tag
      val tag = readUntilMatch(possibleStartTags, false)

      if (fsDataInputStream.getPos < end && tag != null) {
        try {


          dataOutputBuffer.write(tag)

          val endTag = readUntilMatch(possibleEndTags, true)

          if (endTag != null) {

            // Set key as pos of data input stream
            currentKey.set(fsDataInputStream.getPos)

            // Write data from start of start tag to end of end tag
            currentValue.set(dataOutputBuffer.getData, 0, dataOutputBuffer.getLength)
            return true
          }
        }
        finally {
          dataOutputBuffer.reset()
        }
      }
      false
    }


    private def readUntilMatch(tags: Array[Array[Byte]], lookingForEndTag: Boolean): Array[Byte] = {

      // Counter where each index represents a tag and its value represents the byte count
      val matchCounter: Array[Int] = Array.fill(tags.length)(0)

      while (true) {
        // Read a byte from the input stream
        val currentByte = fsDataInputStream.read()

        // Return null if input stream does not get pointed to the first character
        if (currentByte == -1) {
          return null
        }

        // Write from current byte to last byte when looking for the end tag
        if (lookingForEndTag) {
          dataOutputBuffer.write(currentByte)
        }

        // Iterate through all possible tags
        tags.indices.foreach { tagIndex =>

          // Extract start tag to check
          val tag = tags(tagIndex)


          if (currentByte == tag(matchCounter(tagIndex))) {
            matchCounter(tagIndex) += 1

            // If no. of bytes match then the tag matches
            if (matchCounter(tagIndex) >= tag.length) {
              return tag
            }
          }
          else {
            matchCounter(tagIndex) = 0 // Reset no. of bytes in case of mismatch in no. of bytes
          }
        }

        // Return null in case all checks fail and we are looking for the start tag
        if (!lookingForEndTag && matchCounter.forall(_ == 0) && fsDataInputStream.getPos >= end) {
          return null
        }
      }
      null
    }


    override def getCurrentKey: LongWritable = {
      new LongWritable(currentKey.get())
    }

    override def getCurrentValue: Text = {
      new Text(currentValue)
    }

    override def getProgress: Float = (fsDataInputStream.getPos - start) / (end - start).toFloat

    override def close(): Unit = Closeables.close(fsDataInputStream, true)
  }

}


