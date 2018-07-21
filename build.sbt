name := "spark-streaming-ignite-example"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming-kafka-0-10-assembly" % "2.3.0",
  "org.apache.spark" %% "spark-streaming" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0",
  "org.apache.ignite" % "ignite-spark" % "2.6.0"
)



