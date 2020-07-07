package com.lyq.flink.transformation

import com.lyq.flink.source.StationLog
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TestProcessFunction {

  //station_0,18600003503,18900009859,fail,157080459130,0
  //station_0,18600003503,18900009859,success,157080459130,30
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[StationLog] = streamEnv.socketTextStream("node1",9999)
      .map(line=> {
        var arr = line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })

    val result: DataStream[String] = stream.keyBy(_.callIn).process(new MonitorCallFail)
    result.print("被叫连续5秒失败")

    streamEnv.execute()

  }

  class MonitorCallFail extends KeyedProcessFunction[String, StationLog, String]{
    // 使用一个状态对象记录时间
    lazy val timeState: ValueState[Long] =
      getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))

    override def processElement(value: StationLog, ctx: KeyedProcessFunction[String, StationLog, String]#Context, out: Collector[String]): Unit = {
      // 从状态中取得时间
      var time = timeState.value()
      if(time == 0 && value.callType.equals("fail")){ //表示第一次发现呼叫失败，记录当前时间
        // 获取当前系统时间，并注册定时器
        var nowTime = ctx.timerService().currentProcessingTime()
        // 定时器在5秒后触发
        var onTime = nowTime+5*1000L
        ctx.timerService().registerProcessingTimeTimer(onTime)
        // 把触发的时间保存到状态中
        timeState.update(onTime)
      }

      if(time != 0 && !value.callType.equals("fail")){  //表示有一次成功的呼叫，必须要删除定时器
        ctx.timerService().deleteProcessingTimeTimer(time)
        timeState.clear() //清空状态中的时间
      }
    }

    // 时间到了，定时器执行
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext, out: Collector[String]): Unit = {
      var warnStr = "触发的时间：" + timestamp + " 手机号：" + ctx.getCurrentKey
      out.collect(warnStr)
      timeState.clear()
    }

  }

}
