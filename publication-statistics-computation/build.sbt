name := "publication-statistics-computation"

version := "0.1"

scalaVersion := "2.12.8"

sbtVersion := "1.1.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.apache.httpcomponents" % "httpclient" % "4.5",
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.scalamock" %% "scalamock" % "4.0.0" % "test",
  "org.apache.hadoop" % "hadoop-common" % "3.2.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.0",
  "org.scala-lang.modules" %% "scala-xml" % "1.1.1"
)


mainClass in(Compile, run) := Some("main.scala.com.publication.statistics.computation.MapReduceJobsDriver")
mainClass in assembly := Some("main.scala.com.publication.statistics.computation.MapReduceJobsDriver")

