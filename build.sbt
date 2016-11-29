name := "znap"

version := sys.props.getOrElse("version", default = "1.0-SNAPSHOT")

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-feature",
  "-unchecked",
  "-deprecation"
)

val akkaVersion = "2.4.14"
libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
//    "com.typesafe.akka" %% "akka-http-core" % akkaVersion,
//    "com.typesafe.akka" %% "akka-http-experimental" % akkaVersion,
    "com.typesafe.akka" %% "akka-http" % "10.0.0",
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,

    "ch.qos.logback" % "logback-classic" % "1.1.7",

    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.7.7",
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.7.7",
//    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",

    "org.zalando" % "scarl_2.11" % "0.1.4",

    // Typesafe Config is included in Akka, but not necessarily the latest version.
    "com.typesafe" % "config" % "1.3.0",

    "org.zalando.stups" % "tokens" % "0.9.9",
    // Required for stups tokens
//    "org.slf4j" % "slf4j-simple" % "1.7.21",
    "org.apache.httpcomponents" % "httpclient" % "4.5.2",

    "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.35",
    "com.amazonaws" % "aws-java-sdk-s3" % "1.11.35",
    "com.amazonaws" % "aws-java-sdk-sqs" % "1.11.35",
    "com.amazonaws" % "aws-java-sdk-kinesis" % "1.11.26",
    "com.amazonaws" % "amazon-kinesis-producer" % "0.12.0",

    "org.apache.commons" % "commons-compress" % "1.12",

    "nl.grons" %% "metrics-scala" % "3.5.5_a2.3",

    "org.scalatest" %% "scalatest" % "3.0.0" % "test"
  )
}
