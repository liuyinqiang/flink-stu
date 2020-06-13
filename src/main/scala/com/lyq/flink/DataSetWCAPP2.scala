package com.lyq.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object DataSetWCAPP2 {

  def main(args: Array[String]): Unit = {

    val tool = ParameterTool.fromArgs(args)
    val input_path = tool.get("input")
    val output_path = tool.get("output")

    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet : DataSet[String] = env.readTextFile(input_path)
    val aggSet : AggregateDataSet[(String, Int)] = dataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    aggSet.writeAsText(output_path).setParallelism(1)
    env.execute()

  }
}
