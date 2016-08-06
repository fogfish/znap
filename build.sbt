name := "znap"

version := sys.props.getOrElse("version", default = "1.0-SNAPSHOT")

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-feature",
  "-unchecked",
  "-deprecation"
)

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.4.7",
    "com.typesafe.akka" %% "akka-stream" % "2.4.7",
    "com.typesafe.akka" %% "akka-http-core" % "2.4.7",
    "com.typesafe.akka" %% "akka-http-experimental" % "2.4.7",
    "com.typesafe.akka" %% "akka-slf4j" % "2.4.7",

    "ch.qos.logback" % "logback-classic" % "1.1.7",

    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.7.4",
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.7.4",
//    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",

//    "org.slf4j" % "jcl-over-slf4j" % "1.7.18",

    // Typesafe Config is included in Akka, but not necessarily the latest version.
    "com.typesafe" % "config" % "1.3.0",

    "org.zalando.stups" % "tokens" % "0.9.9",
    "org.zalando" % "scarl_2.11" % "0.1.4",
    // Required for stups tokens
//    "org.slf4j" % "slf4j-simple" % "1.7.21",
    "org.apache.httpcomponents" % "httpclient" % "4.5.2",

    "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.21",
    "com.amazonaws" % "aws-java-sdk-s3" % "1.11.22",

    "org.apache.commons" % "commons-compress" % "1.12",

    "org.scalatest" %% "scalatest" % "2.2.6" % "test"
  )
}
