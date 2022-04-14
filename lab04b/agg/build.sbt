ThisBuild / version := "1.0`"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "agg"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5"

