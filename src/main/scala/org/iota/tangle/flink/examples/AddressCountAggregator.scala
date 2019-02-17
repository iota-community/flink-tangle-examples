package org.examples

import org.apache.flink.api.common.functions.AggregateFunction

class AddressCountAggregator extends AggregateFunction[(String, Long), (String, Long), (String, Long)]
{
  override def add(value: (String, Long), accumulator: (String, Long)): (String, Long) =
    (value._1, value._2 + accumulator._2)

  override def createAccumulator(): (String, Long) = ("", 0L)

  override def getResult(accumulator: (String, Long)): (String, Long) = accumulator

  override def merge(a: (String, Long), b: (String, Long)): (String, Long) = (a._1, a._2 + b._2)
}
