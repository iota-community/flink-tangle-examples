package org.iota

import org.apache.flink.api.common.functions.AggregateFunction

class MostUsedAddressesAggregator(number: Int) extends AggregateFunction[(String, Long), Map[String, Long], List[String, Long]]
{

  override def add(value: (String, Long), accumulator: Map[String, Long]): Map[String, Long] = {
    if(accumulator.keys.exists(value._1 == _)){

    }else{
      accumulator
    }
  }

  override def createAccumulator(): Map[String, Long] = Map()

  override def getResult(accumulator: Map[String, Long]): List[(String, Long)] =
    accumulator.toList.sortBy(_._2).take(number)

  override def merge(a: Map[String, Long], b: Map[String, Long]): Map[String, Long] = {
    val seq = a.toSeq ++ b.toSeq
    val grouped = seq.groupBy(_._1)
    val mapWithCounts = grouped.map{case (key, value) => (key, value.map(_._2))}

    mapWithCounts.map{case (key, value) => (key, value.sum)}
  }
}
