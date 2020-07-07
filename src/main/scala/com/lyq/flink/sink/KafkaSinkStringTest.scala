package com.lyq.flink.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaSinkStringTest {

  //linux
  //nc -lp 9999
  //kafka-console-consumer.sh --bootstrap-server node1:9092,node2:9092,node3:9092 --topic topic_tst --from-beginning
  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[String] = streamEnv.socketTextStream("node1", 9999)

    val words: DataStream[String] = stream.flatMap(_.split(" "))
    words.addSink(new FlinkKafkaProducer[String]("node1:9092,node2:9092,node3:9092","topic_tst",new SimpleStringSchema()))

    streamEnv.execute("kafkaSinkStringTest")
  }

}
