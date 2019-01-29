package com.gameole

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import com.gameole.iri.stream.{IRIStream, ServerConnectionConf}
import com.gameole.iri.stream.messages.transactionMessages.{ConfirmedTransactionMessage, UnconfirmedTransactionMessage}
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scalapb.GeneratedMessage

/**
  * Skeleton for a Flink Job.
  *
  * For a full example of a Flink Job, see the WordCountJob.scala file in the
  * same package/directory or have a look at the website.
  *
  * You can also generate a .jar file that you can submit on your Flink
  * cluster. Just type
  * {{{
  *   sbt clean assembly
  * }}}
  * in the projects root directory. You will find the jar in
  * target/scala-2.11/Flink\ Project-assembly-0.1-SNAPSHOT.jar
  *
  */

class CountFunction extends ProcessFunction[UnconfirmedTransactionMessage, String] {
  lazy val state: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("myState", classOf[Int]))

  override def processElement(
                               value: UnconfirmedTransactionMessage,
                               ctx: ProcessFunction[UnconfirmedTransactionMessage, String]#Context,
                               out: Collector[String]): Unit = {

    val current: Int = state.value() + 1

    state.update(current)

    ctx.timerService().registerEventTimeTimer(10000)
  }

  override def onTimer(
                        timestamp: Long, ctx: ProcessFunction[UnconfirmedTransactionMessage, String]#OnTimerContext,
                        out: Collector[String]
                      ): Unit = {
    print("GETTING STATE")
    println(state.value())
    out.collect(state.value().toString)
  }
}

class BundleSplitProcessor extends ProcessFunction[UnconfirmedBundle, TransactionBundle] {
  override def processElement(
                               value: UnconfirmedBundle,
                               ctx: ProcessFunction[UnconfirmedBundle, TransactionBundle]#Context,
                               out: Collector[TransactionBundle]): Unit = {
    val bundle = value
    lazy val elementsCount = bundle.input.length + bundle.output.length
    if (bundle.input.nonEmpty && bundle.size != elementsCount) {

      val inputCount = bundle.input.length / bundle.size
      val groupedInput = bundle.input.sortBy(_.indexInBundle).grouped(inputCount)

      val outputCount = bundle.output.length / bundle.size
      val groupedOutput = bundle.output.sortBy(_.indexInBundle).grouped(outputCount)

      val elements =
        for {
          inputs <- groupedInput
          outputs <- groupedOutput
        } yield {
          ReattachmentBundle(
            bundle.hash,
            bundle.amount,
            inputs,
            outputs,
            bundle.size
          )
        }

      val head = elements.take(1).map(reattached =>
        UnconfirmedBundle(
          reattached.hash, reattached.amount,
          reattached.input, reattached.output,
          reattached.size
        )
      ).toList
      val tail = elements.take(elementsCount / bundle.size).toList

      head.foreach(out.collect)
      tail.foreach(out.collect)
    } else {
      out.collect(bundle)
    }
  }
}

class BundleAggregator extends AggregateFunction[UnconfirmedTransactionMessage, UnconfirmedBundle, UnconfirmedBundle] {

  override def add(value: UnconfirmedTransactionMessage, accumulator: UnconfirmedBundle): UnconfirmedBundle = {
    val bundleHash = value.bundleHash
    val amount = if (value.amount > 0) accumulator.amount + value.amount else accumulator.amount
    val size = value.maxIndexInBundle + 1

    if (value.amount < 0) {
      UnconfirmedBundle(
        bundleHash, amount,
        accumulator.input :+ value,
        accumulator.output,
        size
      )
    }
    else {
      UnconfirmedBundle(
        bundleHash, amount,
        accumulator.input,
        accumulator.output :+ value,
        size
      )
    }
  }

  override def createAccumulator(): UnconfirmedBundle = UnconfirmedBundle("", 0L, List(), List(), 0)

  override def getResult(accumulator: UnconfirmedBundle): UnconfirmedBundle = accumulator

  override def merge(a: UnconfirmedBundle, b: UnconfirmedBundle): UnconfirmedBundle =
    UnconfirmedBundle(a.hash, a.amount + b.amount, a.input ++ b.input, a.output ++ b.output, b.size)
}

trait TransactionBundle {
  val hash: String
  val amount: Long
  val input: List[UnconfirmedTransactionMessage]
  val output: List[UnconfirmedTransactionMessage]
  val size: Int
}

case class UnconfirmedBundle(
                              hash: String,
                              amount: Long,
                              input: List[UnconfirmedTransactionMessage],
                              output: List[UnconfirmedTransactionMessage],
                              size: Int
                            ) extends TransactionBundle

case class ReattachmentBundle(
                               hash: String,
                               amount: Long,
                               input: List[UnconfirmedTransactionMessage],
                               output: List[UnconfirmedTransactionMessage],
                               size: Int
                             ) extends TransactionBundle

object Job {
  def main(args: Array[String]) {

    val config = ConfigFactory.load()

    val zeroMQHost = config.getString(ConfigurationKeys.ZeroMQ.host)
    val zeroMQPort = config.getInt(ConfigurationKeys.ZeroMQ.port)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new IRISource(zeroMQHost, zeroMQPort, ""))

    /*val keyedStream = stream
      .filter(_.isInstanceOf[UnconfirmedTransactionMessage])
      .map(_.asInstanceOf[UnconfirmedTransactionMessage])
      .keyBy(_.bundleHash)*/

    //keyedStream.timeWindow(Time.seconds(10)).sum("amount").print()

    val countStream = stream
      .filter(_.isInstanceOf[UnconfirmedTransactionMessage])
      .map(_.asInstanceOf[UnconfirmedTransactionMessage])
      .keyBy(_.bundleHash)
      .timeWindow(Time.seconds(20))
      .aggregate(new BundleAggregator)
      .flatMap(bundle => {
        lazy val elementsCount = bundle.input.length + bundle.output.length

        if(elementsCount > bundle.size){

        }

        List()
      }
      )


    /*
    .map(tx => {
          if(tx.amount > 0)
            UnconfirmedBundle(tx.bundleHash, tx.amount, List(tx))
          else
            UnconfirmedBundle(tx.bundleHash, 0, List(tx))
      })
      .reduce((c, n) => {
        UnconfirmedBundle(c.hash, c.amount + n.amount, c.transactions ++ n.transactions)
      })*/


    countStream.map(m => {
      println("BUNDLE HASH: " + m.hash)
      println("AMOUNT: " + m.amount)
      println("INPUT COUNT: " + m.input.length)
      println("OUTPUT COUNT: " + m.input.length)

      m
    })

    // execute program
    env.execute("Flink Scala API Skeleton")
  }

}
