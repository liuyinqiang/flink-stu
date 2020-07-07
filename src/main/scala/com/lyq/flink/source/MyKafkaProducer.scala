package com.lyq.flink.source

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.util.Random

object MyKafkaProducer {

  def main(args: Array[String]): Unit = {

    //连接kafka的属性
    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.serializer",classOf[StringSerializer].getName)
    props.setProperty("value.serializer",classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)
    var r = new Random()
    while(true){
      val currData = new ProducerRecord[String, String]("topic_test", "key"+r.nextInt(10), "value"+ r.nextInt(100))
      println(currData.key()+"-"+currData.value())
      producer.send(currData)
      Thread.sleep(1000)
    }

  }
}
