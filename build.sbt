name := "theheat-analysis"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.1",
  "io.spray" %%  "spray-json" % "1.3.3"
)