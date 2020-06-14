package com.lyq.flink.source

import org.apache.flink.streaming.api.scala._

object HDFSFileSource {


  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = streamEnv.readTextFile("hdfs://node1:9000/test/wc.txt")

    val result = stream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
    result.print()

    streamEnv.execute("wordcount")

  }
}
