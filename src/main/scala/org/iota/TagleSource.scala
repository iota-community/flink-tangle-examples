package org.iota

import org.apache.flink.api.common.functions.StoppableFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.iota.tangle.stream.{ZeroMQMessage, ZeroMQMessageParser}
import org.zeromq.ZMQ
import org.zeromq.ZMQ.Socket
import scalapb.GeneratedMessage

import scala.annotation.tailrec

class TagleSource(host: String, port: Int, topic: String) extends RichSourceFunction[GeneratedMessage] with StoppableFunction{
  private var subscriber: Socket = _
  private var context: ZMQ.Context = _
  private var parser: ZeroMQMessageParser = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    this.parser = new ZeroMQMessageParser
    this.context = ZMQ.context(1)
    this.subscriber = context.socket(ZMQ.SUB)

    subscriber.connect(s"tcp://$host:$port")
  }

  override def run(ctx: SourceFunction.SourceContext[GeneratedMessage]): Unit = {
    subscriber.subscribe(topic.getBytes)
    createMessageStream(ctx)
  }

  override def close(): Unit = {
    super.close()
    subscriber.close()
    context.term()
  }

  override def cancel(): Unit = close()


  override def stop(): Unit = close()


  @tailrec
  private def createMessageStream(ctx: SourceFunction.SourceContext[GeneratedMessage]): Unit = {
    val message: Array[String] = subscriber.recv(0).map(_.toChar).mkString.split(" ")
    val zeroMQMessage: ZeroMQMessage = new ZeroMQMessage(message.head, message.tail.toList)

    val maybeProtoMessage = parser.parse(zeroMQMessage)
    maybeProtoMessage match {
      case Some(protoMessage) =>
        ctx.collect(protoMessage)
      case None =>
        println("ERROR PARSING MESSAGE")
        println(zeroMQMessage.messageType)
        println(zeroMQMessage.message)
    }

    createMessageStream(ctx)
  }

}
