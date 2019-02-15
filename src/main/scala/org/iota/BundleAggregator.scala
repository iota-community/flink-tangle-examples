package org.iota

import org.apache.flink.api.common.functions.AggregateFunction
import org.iota.tangle.stream.messages.transactionMessages.UnconfirmedTransactionMessage

class BundleAggregator extends AggregateFunction[UnconfirmedTransactionMessage, UnconfirmedBundle, UnconfirmedBundle] {

  override def add(value: UnconfirmedTransactionMessage, accumulator: UnconfirmedBundle): UnconfirmedBundle = {
    val bundleHash = value.bundleHash
    val amount = if (value.amount > 0) accumulator.amount + value.amount else accumulator.amount
    val size = value.maxIndexInBundle + 1

    if (value.amount <= 0) {
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
