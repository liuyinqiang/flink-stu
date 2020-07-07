package com.lyq.flink.sink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {

  //linux
  //nc -lp 9999
  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[String] = streamEnv.socketTextStream("node1", 9999)

    val result = stream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    val config = new FlinkJedisPoolConfig.Builder().setDatabase(0).setHost("node1").setPort(6379).build()

    result.addSink(new RedisSink[(String, Int)](config, new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET,"t_wc")
      }

      // 从数据中获取key
      override def getKeyFromData(data: (String, Int)): String = {
        data._1
      }
      // 从数据中获取value
      override def getValueFromData(data: (String, Int)): String = {
        data._2.toString
      }

    }))

    streamEnv.execute("redisSinkTest")

  }

}
