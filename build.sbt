name := "scala-nsq"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "io.netty" % "netty-all" % "4.0.40.Final"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.21" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.2" % "test"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.4.0"