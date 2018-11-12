import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  lazy val mockito = "org.mockito" % "mockito-all" % "1.9.5" % "test"
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided"
  lazy val sparkMlLib = "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided"
  lazy val sparkStreaming = "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided"
  lazy val sparkGraphx = "org.apache.spark" %% "spark-graphx" % "2.2.0" % "provided"
//  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "2.2.0"
//  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.2.0"
//  lazy val sparkMlLib = "org.apache.spark" %% "spark-mllib" % "2.2.0"
//  lazy val sparkStreaming = "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
  lazy val scalaCsv = "com.github.tototoshi" %% "scala-csv" % "1.3.5"
  lazy val scallop = "org.rogach" %% "scallop" % "3.1.0"
  lazy val commonsMath = "org.apache.commons" % "commons-math3" % "3.0"
  lazy val log4jApi = "org.apache.logging.log4j" % "log4j-api" % "2.11.1"
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % "2.11.1"
}
