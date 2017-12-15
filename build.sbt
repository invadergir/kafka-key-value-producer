name := "kafka-key-value-producer"

version := "0.1"

organization := "com.example"

scalaVersion := "2.12.4"

val json4SVer = "3.5.3"
val kafkaVer = "1.0.0"
val scalatestVer = "3.0.4"

// Always fork the jvm (test and run)
//fork := true

libraryDependencies ++= Seq(
  
  // For JSON parsing (see https://github.com/json4s/json4s)
  "org.json4s" %%  "json4s-jackson" % json4SVer,
  "org.json4s" %%  "json4s-ext" % json4SVer,
  
  // Kafka
  "org.apache.kafka" % "kafka-clients" % kafkaVer,
  
  // logging
  "ch.qos.logback" % "logback-classic" % "1.1.7", // TODO try 1.2.3 latest as of 6-17-2017
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",

  // For testing:
  "org.scalatest" %% "scalatest" % scalatestVer % "test"
//"org.scalactic" %% "scalactic" % scalatestVer % "test",
)

// Print full stack traces in tests:
testOptions in Test += Tests.Argument("-oF")
