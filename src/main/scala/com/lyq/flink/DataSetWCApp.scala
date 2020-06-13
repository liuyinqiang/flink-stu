package com.lyq.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object DataSetWCApp {

  def main(args: Array[String]): Unit = {

    val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val txtDataSet : DataSet[String] = env.readTextFile("D:\\App\\test\\README.md")
    val aggSet : AggregateDataSet[(String, Int)] = txtDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    aggSet.print()

  }
}
