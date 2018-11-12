import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "io.hops",
      scalaVersion := "2.11.8",
      version      := "0.8.0-SNAPSHOT"
    )),
    resolvers +=  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    name := "hops-examples.hops-examples-featurestore",
    libraryDependencies ++= Seq(
      scalaTest,
      sparkCore,
      sparkSql,
      sparkMlLib,
      sparkStreaming,
      scalaCsv,
      scallop,
      commonsMath,
      log4jApi,
      log4jCore
    )
  )

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
