package com.lyq.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaSource {

  //linux kafka producer
  //kafka-console-producer.sh --broker-list node1:9092,node2:9092,node3:9092 --topic topic_test
  def main(args: Array[String]): Unit = {

    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    // props.setProperty("group.id","flink-kafka-test-01")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)

    val stream: DataStream[String] = streamEnv.addSource(new FlinkKafkaConsumer[String]("topic_test", new SimpleStringSchema(), props))
    stream.print()

    streamEnv.execute("flink-kafka-consumer")

  }
}
