package org.iota.tangle.flink.examples

import com.typesafe.config.ConfigFactory
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.iota.tangle.stream.messages.transactionMessages.UnconfirmedTransactionMessage
import org.apache.flink.streaming.api.scala._
import org.iota.tangle.flink.{BundleAggregator, BundleSplitProcessor, TangleSource}

object BundleAggregation {

  def main(args: Array[String]) {
    val unconfirmedMessageDescriptorName = UnconfirmedTransactionMessage.scalaDescriptor.fullName

    val config = ConfigFactory.load()

    val zeroMQHost = config.getString(ConfigurationKeys.ZeroMQ.host)
    val zeroMQPort = config.getInt(ConfigurationKeys.ZeroMQ.port)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new TangleSource(zeroMQHost, zeroMQPort, ""))

    stream
      .filter(_.companion.scalaDescriptor.fullName == unconfirmedMessageDescriptorName)
      .map(_ match {
        case m: UnconfirmedTransactionMessage => Some(m)
        case _ => None
      })
      .map(_.get)
      .keyBy(_.bundleHash)
      .timeWindow(Time.seconds(60))
      .aggregate(new BundleAggregator)
      .process(new BundleSplitProcessor)
      .filter(_.amount >= 100).print()

    // execute program
    env.execute("Bundle aggregation example")
  }
}
