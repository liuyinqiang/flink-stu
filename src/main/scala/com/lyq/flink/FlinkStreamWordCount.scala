package com.lyq.flink

import org.apache.flink.streaming.api.scala._

object FlinkStreamWordCount {

  //linux
  //nc -lp 8888

  def main(args: Array[String]): Unit = {

    // 1. 初始化Flink流计算环境
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream : DataStream[String] = env.socketTextStream("master01", 8888)
    val sumDsStream : DataStream[(String,Int)] = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0) //分组算子：0或者1代表下标。前面DataStream[二元组]，0代表单词，1代表单词出现的次数
      .sum(1) //聚合累计算子
    sumDsStream.print()

    env.execute("wordcount")

  }
}
