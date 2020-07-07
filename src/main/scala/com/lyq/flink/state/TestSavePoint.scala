package com.lyq.flink.state

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object TestSavePoint {

  //liunx
  //./flink list  --查找到任务ID（56003a833cd7435bcd9dd86672d7bc65）
  //./flink savepoint 56003a833cd7435bcd9dd86672d7bc65  --手动执行保存

  //./flink cancel 56003a833cd7435bcd9dd86672d7bc65   --手动执行命令取消任务
  //./flink run -d -s hdfs://node1:9000/flink/checkpoint/savepoints/savepoint-56003a-7385d5fe6307 -c com.lyq.flink.state.TestSavePoint /home/flink/flink-stu-1.0-SNAPSHOT.jar
  def main(args: Array[String]): Unit = {
    val streamEnv : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream : DataStream[String] = streamEnv.socketTextStream("node1", 9999).uid("socket001")
    val stream : DataStream[(String,Int)] = dataStream.flatMap(_.split(" "))
      .uid("flatmap001")
      .filter(_.nonEmpty)
      .map((_,1)).setParallelism(2).uid("map001")
      .keyBy(0)
      .sum(1).setParallelism(2).uid("sum001")

    stream.print().setParallelism(1)

    streamEnv.execute()
  }
}
