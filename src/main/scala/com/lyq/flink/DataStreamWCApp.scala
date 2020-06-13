package com.lyq.flink

import org.apache.flink.streaming.api.scala._

object DataStreamWCApp {
  //windows cmd window
  //nc -l -p 9999

  def main(args: Array[String]): Unit = {

    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream : DataStream[String] = env.socketTextStream("127.0.0.1", 9999)
    val sumDsStream : DataStream[(String,Int)] = dataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)
    sumDsStream.print()

    env.execute()
  }
}
