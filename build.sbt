name := "annoy4s"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)