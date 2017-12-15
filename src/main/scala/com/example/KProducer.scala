package com.example

// Quick and dirty kafka producer singleton

import com.typesafe.scalalogging.Logger
import java.util.Properties
import java.time._
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.collection.JavaConversions._

import com.example.util._
import com.example.util.PropertiesUtil._

object KProducer {

  private val log = Logger("KProducer")

  // create the kafka producer
  private lazy val pConfig = new Properties()

  private var producer: Option[KafkaProducer[String, String]] = None

  // Call this once to configure the producer.
  // Only the first time goes through.
  def configure(bootstrapServerAndPort: String) = {

    if (producer.isEmpty) {
      pConfig.put("bootstrap.servers", bootstrapServerAndPort)
      pConfig.put("acks", "all")
      pConfig.putv("retries", 0)
      pConfig.putv("batch.size", 16384)
      pConfig.putv("linger.ms", 1)
      pConfig.putv("buffer.memory", 33554432)
      pConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      pConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      producer = Option(new KafkaProducer[String, String](pConfig))
    }
    else {
      log.warn(s"Called configure() more than once!  bootstrapServerAndPort = [{}]", bootstrapServerAndPort)
    }
  }


  /** Send a message on a topic with a specific key
    */
  def send(topic: String, key: String, value: String) = {
    producer.getOrElse(throw new RuntimeException("Need to configure the KProducer object!"))
    producer.get.send(new ProducerRecord[String, String](topic, key, value))
  }

  /** close down the producer
    */
  def close() = {
    for (p <- producer)
      p.close()

    producer = None
  }
}

