name := "ann4s"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.3.0" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

organization := "com.github.mskimm"

licenses += "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html")

