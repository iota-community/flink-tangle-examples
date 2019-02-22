package org.iota.tangle.flink.examples

import com.typesafe.config.ConfigFactory
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.iota.tangle.flink.TagleSource
import org.iota.tangle.stream.messages.transactionMessages.UnconfirmedTransactionMessage

object MostUsedAddresses {

  def main(args: Array[String]) {

    val unconfirmedMessageDescriptorName = UnconfirmedTransactionMessage.scalaDescriptor.fullName

    val config = ConfigFactory.load()

    val zeroMQHost = config.getString(ConfigurationKeys.ZeroMQ.host)
    val zeroMQPort = config.getInt(ConfigurationKeys.ZeroMQ.port)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new TagleSource(zeroMQHost, zeroMQPort, ""))

    val unconfirmedTransactionStream = stream
      .filter(_.companion.scalaDescriptor.fullName == unconfirmedMessageDescriptorName)
      .map(_ match {
        case m: UnconfirmedTransactionMessage => Some(m)
        case _ => None
      }).map(_.get)

    val addressOnlyStream = unconfirmedTransactionStream.map(e => (e.address, 1L))

    val keyedStream = addressOnlyStream.keyBy(_._1)

    val keyedTimedWindow = keyedStream.timeWindow(Time.minutes(60), Time.seconds(30))

    //val aggregatedKeyedTimeWindow = keyedTimedWindow.aggregate(new AddressCountAggregator)
    val aggregatedKeyedTimeWindow = keyedTimedWindow.reduce((a, b) => (a._1, a._2 + b._2))


    val timeWindowAll = aggregatedKeyedTimeWindow
        .timeWindowAll(Time.seconds(1))

    val mostUsedStream = timeWindowAll.aggregate(new MostUsedAddressesAggregator(10))

    mostUsedStream.print()

    // execute program
    env.execute("Most used addresses")
  }
}
