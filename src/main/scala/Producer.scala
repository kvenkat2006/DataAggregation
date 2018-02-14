/**
  * Created by Dheefinity on 2018-01-10.
  */


package com.dhee

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random
import scala.io.Source

//RanSud: Typesafe changes next 1 line
import com.typesafe.config.ConfigFactory

object Producer extends App {

//RanSud: Typesafe changes next 1 line
  val config = ConfigFactory.parseFile(new java.io.File("src/main/scala/dataagg.conf"))

  //val isBase = args(0).toUpperCase.compareTo("BASE")==0
  val baseOrWhatif = args(0)

//Ranjini: Typesafe changes next 4 lines
//  val topic = args(1)
  val topic = config.getString("dataagg.config.general.kafkatopic")
//  val brokers = args(2)
  val brokers = config.getString("dataagg.config.general.kafkabroker")

  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

//RanSud: Typesafe changes next 4 lines
//val sourceFilePath = args(3)
  val baseFilePath = config.getString("dataagg.config.producer.basefile")
  val wiFilePath = config.getString("dataagg.config.producer.wifile")
  val sourceFilePath = if (args(0) == "BASE") baseFilePath else wiFilePath

  val producer = new KafkaProducer[String, String](props)
  val ttttt = System.currentTimeMillis()

  var countOfRecordsPublished = 0;

  val bufferedSource = Source.fromFile(sourceFilePath)
  for (line <- bufferedSource.getLines) {
    val splitLine = line.split(",")
    val appendedLine = line + "," + baseOrWhatif
    val data = new ProducerRecord[String, String](topic, splitLine(1), appendedLine)
    println(splitLine(1) + " --> " + appendedLine)
    producer.send(data)
    countOfRecordsPublished = countOfRecordsPublished +1
  }

  val finishTime = System.currentTimeMillis()

  bufferedSource.close

  System.out.println("Published " + countOfRecordsPublished + " messages in " + (finishTime - ttttt) + " milliseconds." )
  System.out.println("Rate of publishing of messages (per second): " + countOfRecordsPublished * 1000 / (finishTime - ttttt))

  producer.close()
}

