package com.lyq.flink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.lyq.flink.source.CustomerSource.MyCustomerSource
import com.lyq.flink.source.StationLog
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object CustomerSinkTest {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)

    stream.addSink(new MyCustomerJdbcSink)

    streamEnv.execute("customerSinkTest")
  }

  /**
    * 自定义的Sink
    */
  class MyCustomerJdbcSink extends RichSinkFunction[StationLog]{

    var conn: Connection = _
    var pst: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test","root","root123")
      pst = conn.prepareCall("insert into t_station_log (sid, call_out, call_in, call_type, call_time, duration) values (?,?,?,?,?,?) ")
    }

    // 把StationLog对象写入MySql表中，每写入一条执行一次
    override def invoke(value: StationLog, context: SinkFunction.Context[_]): Unit = {
      pst.setString(1, value.sid)
      pst.setString(2, value.callOut)
      pst.setString(3, value.callIn)
      pst.setString(4, value.callType)
      pst.setLong(5, value.callTime)
      pst.setLong(6, value.duration)
      pst.executeUpdate()
    }

    // Sink初始化的时候调用一次，一个并行度初始化一次
    // 创建连接对象 和 Statement对象
    override def close(): Unit = {
      pst.close()
      conn.close()
    }

  }
}
