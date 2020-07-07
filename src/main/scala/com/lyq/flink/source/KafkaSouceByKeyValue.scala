package com.lyq.flink.source

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}

object KafkaSouceByKeyValue {

  def main(args: Array[String]): Unit = {

    //1、初始化Flink流计算环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 修改并行度
    streamEnv.setParallelism(1)

    //2、导入隐式转换
    // 放到该位置，下面class不会自动提示，故移动到顶部

    //连接kafka的属性
    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("group.id","flink002")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset","latest")

    val stream: DataStream[(String, String)] = streamEnv.addSource(new FlinkKafkaConsumer[(String, String)]("topic_test", new MyKafkaReader, props))

    stream.print()

    streamEnv.execute("Kafka_KV")

  }


  class MyKafkaReader extends KafkaDeserializationSchema[(String,String)] {
    override def isEndOfStream(t: (String, String)): Boolean = {
      false
    }

    override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
      if(null != consumerRecord){

        var key: String = null
        var value: String = null
        if(null != consumerRecord.key()){
          key = new String(consumerRecord.key(), "UTF-8")
        }
        if(null != consumerRecord.value()){
          value = new String(consumerRecord.value(), "UTF-8")
        }
        (key, value)
      } else{
        ("null","null")
      }
    }

    override def getProducedType: TypeInformation[(String, String)] = {
      createTuple2TypeInformation(createTypeInformation[String],createTypeInformation[String])
    }

  }

}
