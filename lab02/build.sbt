ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "lab02"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
