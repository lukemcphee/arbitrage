name := "arbitrage"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.apache.spark" %% "spark-graphx" % "3.1.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,

)
