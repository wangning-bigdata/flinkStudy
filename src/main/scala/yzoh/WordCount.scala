package yzoh

import org.apache.flink.streaming.api.scala._

/**
 * Created by wangning on 2021/6/24 10:56.
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream = env.socketTextStream("localhost", 9999, ' ')

    val result = stream
      .flatMap(line => line.split(" "))
        .map(word => (word,1))
        .keyBy(_._1)
        .sum(1)

    result.print()

    env.execute()
  }

}
