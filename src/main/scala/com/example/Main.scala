package com.example

// See also https://github.com/apache/kafka/tree/trunk/examples/src/main/java/kafka/examples

import java.time.LocalDateTime

import com.example.util._
import com.typesafe.scalalogging.Logger

object Main {

  // logging
  LoggerUtil.setRootLogLevel(ch.qos.logback.classic.Level.INFO)
  val log = Logger("Main")

  // Formatter for json4s
  implicit val jsonFormats = org.json4s.DefaultFormats

  case class Message(k: String, v: String)

  KProducer.configure("localhost:9092")

  /** Main method
    */
  def main(args: Array[String]) = {

    val topic = args.headOption.getOrElse("input-topic")    
  
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println(">>>>>> Closing KProducer...")
      KProducer.close()
    }))

    kvInputLoop(topic)
    //sendContinuously(100000, topic)

    log.debug("Closing KafkaProducer...")

    KProducer.close()
  }

  /** Create a key set of the specified size and send values through continuously
    *
    */
  def sendContinuously(numKeys: Long, topic: String) = {
    var map = (0L until numKeys).map{ _ => (java.util.UUID.randomUUID.toString, "1") }.toMap

    val delayMS = 1000

    while (true) {
      scala.io.StdIn.readLine(s"Press ENTER to send $numKeys messages: ")
      map = map.map{ case (k,v) =>
        (k, (v.toLong + 1L).toString )
      }
      map.foreach{ case (k,v) => KProducer.send(topic, k, v) }
    }

  }

  /** Accept input and produce message on the topic
    *
    */
  def kvInputLoop(topic: String): Unit = {

    while (true) {
      val command = scala.io.StdIn.readLine("Enter k,v message (comma-delimited): ")
      val messageRE = """^ *(.*?) *, *(.*) *$""".r
      command.trim match {

        case messageRE(k, v) => {
          println(s"Sending key($k), value($v)...")
          KProducer.send(topic, k, v)
        }         
        case s: String => {
          val k = s
          val v = "."
          println(s"Sending key($k), value($v)...")
          KProducer.send(topic, k, v)
        }
        case other => println("Error - you must enter a comma-separated key, value string")
      }
    }
  }
}
