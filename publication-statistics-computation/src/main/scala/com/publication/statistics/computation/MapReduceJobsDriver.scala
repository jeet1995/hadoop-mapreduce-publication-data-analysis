package main.scala.com.publication.statistics.computation

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import main.scala.com.publication.statistics.computation.bucketingByNumOfNodes.{BucketingByNumNodesMapper, BucketingByNumNodesReducer}
import main.scala.com.publication.statistics.computation.computationOfAuthorshipScore.{AuthorshipScoreMapper, AuthorshipScoreReducer}
import main.scala.com.publication.statistics.computation.maxMedianAvgComputation.{MaxMedianAvgMapper, MaxMedianAvgReducer, MaxMedianAvgWritable}
import main.scala.com.publication.statistics.computation.schema.DBLPXmlInputFormat
import main.scala.com.publication.statistics.computation.sortByNumCoAuthors._
import main.scala.com.publication.statistics.computation.utils.ApplicationConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{FloatWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

import scala.collection.JavaConverters._


/**
  * This singleton class denotes the entry point of the map-reduce which will run a list of jobs
  * as specified in the application.conf file.
  * */
object MapReduceJobsDriver extends LazyLogging {

  def main(args: Array[String]): Unit = {

    if (args.length < 1)
      logger.error("No input path argument supplied, please pass an input path argument")

    val inputPath = args(0)

    val mapReduceJobsPipeline = ConfigFactory.load().getConfig(ApplicationConstants.MAP_REDUCE_JOB_PIPELINE)
    executeJobs(mapReduceJobsPipeline, inputPath)
  }

  private def executeJobs(mapReduceJobsPipeline: Config, inputPath: String): Unit = {

    val mapReduceJobs = mapReduceJobsPipeline.getConfigList(ApplicationConstants.JOBS).asScala

    mapReduceJobs.foreach { mapReduceJob =>

      val configuration = new Configuration
      var job: Job = Job.getInstance

      configuration.setStrings(ApplicationConstants.POSSIBLE_START_TAGS, mapReduceJobsPipeline.getStringList(ApplicationConstants.POSSIBLE_START_TAGS).asScala: _*)
      configuration.setStrings(ApplicationConstants.POSSIBLE_END_TAGS, mapReduceJobsPipeline.getStringList(ApplicationConstants.POSSIBLE_END_TAGS).asScala: _*)
      configuration.set(ApplicationConstants.JOB_NAME, mapReduceJob.getString(ApplicationConstants.JOB_NAME))

      mapReduceJob.getString(ApplicationConstants.JOB_TYPE) match {

        // Starts bucketing map-reduce job
        case ApplicationConstants.BUCKETING =>

          if (mapReduceJob.getString(ApplicationConstants.JOB_NAME) == ApplicationConstants.BUCKETING_BY_NUM_CO_AUTHOR) {
            configuration.setInt(ApplicationConstants.BUCKET_SIZE, mapReduceJob.getInt(ApplicationConstants.BUCKET_SIZE))
          }

          configuration.set(ApplicationConstants.JOB_TYPE, mapReduceJob.getString(ApplicationConstants.JOB_TYPE))
          job = Job.getInstance(configuration)

          job.setJarByClass(this.getClass)
          job.setJobName(mapReduceJob.getString(ApplicationConstants.JOB_NAME))

          job.setMapperClass(classOf[BucketingByNumNodesMapper])
          job.setReducerClass(classOf[BucketingByNumNodesReducer])
          job.setCombinerClass(classOf[BucketingByNumNodesReducer])
          job.setInputFormatClass(classOf[DBLPXmlInputFormat])
          job.setOutputKeyClass(classOf[Text])
          job.setOutputValueClass(classOf[IntWritable])
          job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

          // Set to 1 to collate reduce outputs in one file
          job.setNumReduceTasks(1)

          FileInputFormat.setInputPaths(job, new Path(inputPath))
          FileOutputFormat.setOutputPath(job, new Path(inputPath + mapReduceJob.getString(ApplicationConstants.OUTPUT_PATH_SUFFIX)))

          val outputPath = new Path(inputPath + mapReduceJob.getString(ApplicationConstants.OUTPUT_PATH_SUFFIX))

          outputPath.getFileSystem(configuration).delete(outputPath, true)
          logger.info("Execution of job " + job.getJobName + " has begun.")
          job.waitForCompletion(true)
          logger.info("Execution of job " + job.getJobName + " has ended.")


          // Starts mean-median-max job
        case ApplicationConstants.MEAN_MEDIAN_MAX =>
          job = Job.getInstance(configuration)

          job.setJarByClass(this.getClass)
          job.setJobName(mapReduceJob.getString(ApplicationConstants.JOB_NAME))


          job.setMapperClass(classOf[MaxMedianAvgMapper])
          job.setReducerClass(classOf[MaxMedianAvgReducer])
          job.setInputFormatClass(classOf[DBLPXmlInputFormat])
          job.setMapOutputKeyClass(classOf[Text])
          job.setMapOutputValueClass(classOf[MaxMedianAvgWritable])
          job.setOutputKeyClass(classOf[Text])
          job.setOutputValueClass(classOf[Text])
          job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

          configuration.set(ApplicationConstants.JOB_TYPE, mapReduceJob.getString(ApplicationConstants.JOB_TYPE))


          FileInputFormat.setInputPaths(job, new Path(inputPath))
          FileOutputFormat.setOutputPath(job, new Path(inputPath + mapReduceJob.getString(ApplicationConstants.OUTPUT_PATH_SUFFIX)))
          val outputPath = new Path(inputPath + mapReduceJob.getString(ApplicationConstants.OUTPUT_PATH_SUFFIX))
          outputPath.getFileSystem(configuration).delete(outputPath, true)
          logger.info("Execution of job " + job.getJobName + " has begun.")
          job.waitForCompletion(true)
          logger.info("Execution of job " + job.getJobName + " has ended.")


        // Starts authorship score job
        case ApplicationConstants.AUTHORSHIP_SCORE =>
          job = Job.getInstance(configuration)

          job.setJarByClass(this.getClass)
          job.setJobName(mapReduceJob.getString(ApplicationConstants.JOB_NAME))


          job.setMapperClass(classOf[AuthorshipScoreMapper])
          job.setReducerClass(classOf[AuthorshipScoreReducer])
          job.setCombinerClass(classOf[AuthorshipScoreReducer])
          job.setInputFormatClass(classOf[DBLPXmlInputFormat])
          job.setOutputKeyClass(classOf[Text])
          job.setOutputValueClass(classOf[FloatWritable])
          job.setOutputFormatClass(classOf[TextOutputFormat[Text, FloatWritable]])
          configuration.set(ApplicationConstants.JOB_TYPE, mapReduceJob.getString(ApplicationConstants.JOB_TYPE))


          FileInputFormat.setInputPaths(job, new Path(inputPath))
          FileOutputFormat.setOutputPath(job, new Path(inputPath + mapReduceJob.getString(ApplicationConstants.OUTPUT_PATH_SUFFIX)))
          val outputPath = new Path(inputPath +mapReduceJob.getString( ApplicationConstants.OUTPUT_PATH_SUFFIX))
          outputPath.getFileSystem(configuration).delete(outputPath, true)
          logger.info("Execution of job " + job.getJobName + " has begun.")
          job.waitForCompletion(true)
          logger.info("Execution of job " + job.getJobName + " has ended.")

        // Starts sorting job
        case ApplicationConstants.SORT =>

          val job1 = Job.getInstance(configuration)

          job1.setJarByClass(this.getClass)
          job1.setJobName("sort-first-job")
          job1.setMapperClass(classOf[SortByNumCoAuthorsMapper])
          job1.setReducerClass(classOf[SortByNumCoAuthorsReducer])
          job1.setInputFormatClass(classOf[DBLPXmlInputFormat])
          job1.setMapOutputKeyClass(classOf[Text])
          job1.setMapOutputValueClass(classOf[Text])
          job1.setOutputKeyClass(classOf[Text])
          job1.setOutputValueClass(classOf[IntWritable])

          FileInputFormat.setInputPaths(job1, new Path(inputPath))
          FileOutputFormat.setOutputPath(job1, new Path(inputPath + mapReduceJob.getString("intermediate-sort-output-path-suffix")))

          val outputPath = new Path(inputPath + mapReduceJob.getString("intermediate-sort-output-path-suffix"))
          outputPath.getFileSystem(configuration).delete(outputPath, true)

          logger.info("Execution of job " + job1.getJobName + " has begun.")
          job1.waitForCompletion(true)
          logger.info("Execution of job " + job1.getJobName + " has ended.")

          val job2 = Job.getInstance(configuration)

          job2.setJarByClass(this.getClass)
          job2.setJobName("sort-second-job")
          job2.setMapperClass(classOf[InverseNumCoAuthorsMapper])
          job2.setInputFormatClass(classOf[KeyValueTextInputFormat])
          job2.setMapOutputKeyClass(classOf[IntWritable])
          job2.setMapOutputValueClass(classOf[Text])
          job2.setSortComparatorClass(classOf[DescendingOrderByNumCoAuthorComparator])
          job2.setReducerClass(classOf[InverseNumCoAuthorsReducer])

          // Set to 1 to collate reduce outputs into 1 file
          job2.setNumReduceTasks(1)


          FileInputFormat.setInputPaths(job2, new Path(inputPath + mapReduceJob.getString("intermediate-sort-output-path-suffix")))
          FileOutputFormat.setOutputPath(job2, new Path(inputPath + mapReduceJob.getString( "complete-sort-output-path-suffix")))

          val outputPath2 = new Path(inputPath + mapReduceJob.getString( "complete-sort-output-path-suffix"))
          outputPath2.getFileSystem(configuration).delete(outputPath2, true)

          logger.info("Execution of job " + job2.getJobName + " has begun.")
          job2.waitForCompletion(true)
          logger.info("Execution of job " + job2.getJobName + " has ended.")

      }
    }
  }
}
