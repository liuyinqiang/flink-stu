package com.lyq.flink.state

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object TestCheckPointByHDFS {

  //linux
  //nc -lp 9999
  //程序运行过程中，中断程序，执行如下命令恢复：
  //第一种：
  //./flink run -d -s hdfs://node1:9000/flink/checkpoint/cp1/0604e3f2fa9c6ff1010ca21c93a8dec5/chk-41 -c com.lyq.flink.state.TestCheckPointByHDFS /home/flink/flink-stu-1.0-SNAPSHOT.jar
  //第二种：
  //在flink web Uploaded Jars页面，在Savepoint Path 输入框，
  //输入：hdfs://node1:9000/flink/checkpoint/cp1/0604e3f2fa9c6ff1010ca21c93a8dec5/chk-41
  //然后点击Submit

  def main(args: Array[String]): Unit = {
    val streamEnv : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.enableCheckpointing(5000) //每割5秒开启一次checkpoint
    streamEnv.setStateBackend(new FsStateBackend("hdfs://node1:9000/flink/checkpoint/cp1")) //存放检查点数据

    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    streamEnv.getCheckpointConfig.setCheckpointTimeout(5000)
    streamEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    val dataStream : DataStream[String] = streamEnv.socketTextStream("node1", 9999)
    val stream : DataStream[(String,Int)] = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)
    stream.print()

    streamEnv.execute()
  }
}
