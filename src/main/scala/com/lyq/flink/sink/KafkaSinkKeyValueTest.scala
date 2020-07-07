package com.lyq.flink.sink

import java.lang
import java.util.Properties

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

object KafkaSinkKeyValueTest {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[String] = streamEnv.socketTextStream("node1", 9999)

    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    //连接kafka的属性
    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")

    val kafkaSink = new FlinkKafkaProducer[(String, Int)](
      "topic_tst",
      new KafkaSerializationSchema[(String, Int)] {
        override def serialize(element: (String, Int), timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          new ProducerRecord("topic_tst", element._1.getBytes, (element._2 + "").getBytes)
        }
      },
      props,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE //精确一次
    )

    result.addSink(kafkaSink)

    streamEnv.execute("kafkaSinkKeyValueTest")

  }

}
