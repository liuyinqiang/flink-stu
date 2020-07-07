package com.lyq.flink.sink

import com.lyq.flink.source.CustomerSource.MyCustomerSource
import com.lyq.flink.source.StationLog
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object HDFSSink {

  def main(args: Array[String]): Unit = {

    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)

    //默认一个小时一个目录(分桶)
    //设置一个滚动策略

    val rolling: DefaultRollingPolicy[StationLog, String] = DefaultRollingPolicy.create()
      .withInactivityInterval(30000) //不活动的分桶时间
      .withRolloverInterval(10000) //每两秒生成一个文件
      .build()  //创建

    val hdfsSink: StreamingFileSink[StationLog] = StreamingFileSink.forRowFormat[StationLog](
      new Path("hdfs://node1:9000/MySink001/"),
      new SimpleStringEncoder[StationLog]("UTF-8"))
      .withRollingPolicy(rolling)
      .withBucketCheckInterval(1000)  //检查间隔时间
      .build()

    stream.addSink(hdfsSink)

    streamEnv.execute("hdfsSinkTest")
  }

}
