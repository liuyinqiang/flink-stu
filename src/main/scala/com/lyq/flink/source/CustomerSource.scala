package com.lyq.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object CustomerSource {

  var flag = true;
  val r = new Random();
  val types = Array("fail","busy","barring","success");

  class MyCustomerSource extends SourceFunction[StationLog]{
    override def run(ctx: SourceFunction.SourceContext[StationLog]): Unit = {
      println("--------------")
      while(flag){
        1.to(10).map(i=>{
          var callOut = "1860000%04d".format(r.nextInt(10000))
          var callIn = "1890000%04d".format(r.nextInt(10000))
          new StationLog("station_"+r.nextInt(10),callOut,callIn,types(r.nextInt(4)),System.currentTimeMillis(),100L)
        }).foreach(ctx.collect(_))

        Thread.sleep(2000)
      }


    }

    //终止数据
    override def cancel(): Unit = {
      flag = false;
    }
  }


  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)
    stream.print()

    streamEnv.execute("自定义source")
  }
}
