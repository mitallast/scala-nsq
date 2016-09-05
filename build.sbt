organization := "com.github.mitallast"
name := "scala-nsq"
version := "1.0-SNAPSHOT"

description := "Scala NSQ client"

scalaVersion := "2.11.8"

libraryDependencies += "io.netty" % "netty-all" % "4.0.40.Final"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.21" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.2" % "test"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.4.0"

publishMavenStyle := true

pomIncludeRepository := { _ => false }

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra in Global := {
  <url>https://github.com/mitallast/scala-nsq</url>
    <licenses>
      <license>
        <name>MIT License</name>
        <url>https://github.com/mitallast/scala-nsq/blob/master/LICENSE</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:mitallast/scala-nsq.git</url>
      <connection>scm:git:git@github.com:mitallast/scala-nsq.git</connection>
    </scm>
    <developers>
      <developer>
        <id>mitallast</id>
        <name>Alexey Korchevsky</name>
        <url>https://github.com/mitallast</url>
      </developer>
    </developers>
}