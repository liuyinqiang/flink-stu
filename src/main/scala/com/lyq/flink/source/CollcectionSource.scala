package com.lyq.flink.source

import org.apache.flink.streaming.api.scala._

/**
  * 基站日志
  *
  * @param sid  基站ID
  * @param callOut 主叫号码
  * @param callIn  被叫号码
  * @param callType 呼叫类型
  * @param callTime 呼叫时间(毫秒)
  * @param duration 通话时长(秒)
  */
case class StationLog(sid:String,callOut:String,callIn:String,callType:String,callTime:Long,duration:Long)

object CollcectionSource {

  def main(args: Array[String]): Unit = {

    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    streamEnv.setParallelism(1)

//    streamEnv.fromCollection()
    val stream: DataStream[StationLog] = streamEnv.fromCollection(Array(
      new StationLog("001", "10086", "10010", "busy", 20L, 50L),
      new StationLog("002", "10010", "10010", "success", 20L, 50L),
      new StationLog("003", "10000", "10086", "busy", 20L, 50L),
      new StationLog("004", "10000", "10010", "success", 20L, 50L)
    ))


    stream.print()
    streamEnv.execute()

  }
}
