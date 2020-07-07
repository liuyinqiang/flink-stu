package com.lyq.flink.transformation

import com.lyq.flink.source.CustomerSource.MyCustomerSource
import com.lyq.flink.source.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TestSplitAndSelect {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = streamEnv.addSource(new MyCustomerSource)

    val splitStream: SplitStream[StationLog] = stream.split( //流并没有真正的切割
      log => {
        if (log.callType.equals("success")) Seq("Success") else Seq("No Success") //给两个不同条件的数据打上不同的标签
      }
    )
    val stream1 = splitStream.select("Success")
    val stream2 = splitStream.select("No Success")

    stream1.print("通话成功")
    stream2.print("通话不成功")

    streamEnv.execute()

  }

}
