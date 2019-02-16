package org.iota

import org.apache.flink.api.common.functions.AggregateFunction

class MostUsedAddressesAggregator(number: Int) extends AggregateFunction[(String, Long), Map[String, Long], List[(String, Long)]]
{
  override def add(value: (String, Long), accumulator: Map[String, Long]): Map[String, Long] = {
    accumulator ++ Map(value._1 -> (value._2 + accumulator.getOrElse(value._1, 0L)))
  }

  override def createAccumulator(): Map[String, Long] = Map()

  override def getResult(accumulator: Map[String, Long]): List[(String, Long)] =
    accumulator.toList.sortWith(_._2 > _._2).take(10)

  override def merge(a: Map[String, Long], b: Map[String, Long]): Map[String, Long] = {
    val seq = a.toSeq ++ b.toSeq
    val grouped = seq.groupBy(_._1)
    val mapWithCounts = grouped.map{case (key, value) => (key, value.map(_._2))}

    mapWithCounts.map{case (key, value) => (key, value.sum)}
  }
}
