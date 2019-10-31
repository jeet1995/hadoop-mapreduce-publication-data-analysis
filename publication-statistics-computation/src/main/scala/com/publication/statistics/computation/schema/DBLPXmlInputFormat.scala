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
  * */
class DBLPXmlInputFormat extends TextInputFormat with LazyLogging {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    // Record reader which generates records of various tags.
    new MultiTagXmlRecordReader(split.asInstanceOf[FileSplit], context.getConfiguration)
  }

  class MultiTagXmlRecordReader(split: FileSplit, conf: Configuration)
    extends RecordReader[LongWritable, Text] {

    // Get the start and end tags as specified by the user in the Hadoop job configuration
    private val possibleStartTags = conf.getStrings(ApplicationConstants.POSSIBLE_START_TAGS).map(_.getBytes(StandardCharsets.UTF_8))
    private val possibleEndTags = conf.getStrings(ApplicationConstants.POSSIBLE_END_TAGS).map(_.getBytes(StandardCharsets.UTF_8))

    // Open the file and seek to the start of the split
    private val start = split.getStart
    private val end = start + split.getLength

    private val fsDataInputStream = split.getPath.getFileSystem(conf).open(split.getPath)


    // Buffer for storing file content between the start and end tags
    private val dataOutputBuffer = new DataOutputBuffer()

    // Track the current key and value
    private val currentKey = new LongWritable()
    private val currentValue = new Text()


    override def nextKeyValue(): Boolean = {
      // Keep scanning the input file until one of the start tags is encountered

      val tag = readUntilMatch(possibleStartTags, false)

      if (fsDataInputStream.getPos < end && tag != null) {
        try {
          // Store the matched tag in the buffer (Our output will start with the start tag)
          dataOutputBuffer.write(tag)

          val endTag = readUntilMatch(possibleEndTags, true)

          // Keep reading until the corresponding end tag is matched
          if (endTag != null) {

            // Emit the key and value once the end tag is matched
            currentKey.set(fsDataInputStream.getPos)
            currentValue.set(dataOutputBuffer.getData, 0, dataOutputBuffer.getLength)
            return true
          }
        }
        finally {
          // Reset the buffer so that we start fresh next time
          dataOutputBuffer.reset()
        }
      }
      false
    }


    private def readUntilMatch(tags: Array[Array[Byte]], lookingForEndTag: Boolean): Array[Byte] = {
      // Trackers for the bytes that have been currently matched for each tag. Initialized to 0 at the beginning.
      val matchCounter: Array[Int] = tags.indices.map(_ => 0).toArray

      while (true) {
        // Read a byte from the input stream
        val currentByte = fsDataInputStream.read()

        // Return null if end of file is reached
        if (currentByte == -1) {
          return null
        }

        // If we are looking for the end tag, buffer the file contents until we find it.
        if (lookingForEndTag) {
          dataOutputBuffer.write(currentByte)
        }

        // Check if we are matching any of the tags
        tags.indices.foreach { tagIndex =>
          // The current tag which we are testing for a match
          val tag = tags(tagIndex)

          if (currentByte == (matchCounter(tagIndex))) {
            matchCounter(tagIndex) += 1

            // If the counter for this tag reaches the length of the tag, we have found a match
            if (matchCounter(tagIndex) >= tag.length) {
              return tag
            }
          }
          else {
            // Reset the counter for this tag if the current byte doesn't match with the byte of the current tag being
            // tested
            matchCounter(tagIndex) = 0
          }
        }
        // Check if we've passed the stop point
        if (!lookingForEndTag && matchCounter.forall(_ == 0) && fsDataInputStream.getPos >= end) {
          return null
        }
      }
      null
    }

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      fsDataInputStream.seek(start)
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


