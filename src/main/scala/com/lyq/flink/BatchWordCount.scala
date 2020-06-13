package com.lyq.flink

import org.apache.flink.api.scala._

object BatchWordCount {

  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataPath = getClass.getResource("/wc.txt")

    val data: DataSet[String] = env.readTextFile(dataPath.getPath)

    data.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1).print()
  }
}
