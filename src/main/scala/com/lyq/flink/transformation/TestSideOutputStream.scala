package com.lyq.flink.transformation

import com.lyq.flink.source.StationLog
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TestSideOutputStream {

  val notSuccessTag = new OutputTag[StationLog]("not_success") //不成功的标签

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val data = streamEnv.readTextFile(getClass.getResource("/station.log").getPath)
      .map(line => {
        var arr = line.split(",")
        new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    val result: DataStream[StationLog] = data.process(new CreateSideOutputStream(notSuccessTag))
    result.print("主流")

    val sideStream: DataStream[StationLog] = result.getSideOutput(notSuccessTag)
    sideStream.print("侧流")

    streamEnv.execute()
  }

  class CreateSideOutputStream(tag: OutputTag[StationLog]) extends ProcessFunction[StationLog, StationLog]{
    override def processElement(value: StationLog, ctx: ProcessFunction[StationLog, StationLog]#Context, out: Collector[StationLog]): Unit = {
      if(value.callType.equals("success")){
        out.collect(value)  //输出主流
      }else{
        ctx.output(tag, value)  //输出侧流
      }
    }
  }

}
