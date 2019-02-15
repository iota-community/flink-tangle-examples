package org.iota

import org.iota.tangle.stream.messages.transactionMessages.UnconfirmedTransactionMessage
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class BundleSplitProcessor extends ProcessFunction[UnconfirmedBundle, TransactionBundle] {
  override def processElement(
                               value: UnconfirmedBundle,
                               ctx: ProcessFunction[UnconfirmedBundle, TransactionBundle]#Context,
                               out: Collector[TransactionBundle]): Unit = {
    val bundle = value
    val elementsCount = bundle.input.length + bundle.output.length
    if (bundle.input.nonEmpty && bundle.size > elementsCount) {

      val inputGrouped = bundle.input.groupBy(_.indexInBundle).map{case (_, value) => value}.toList
      val outputGrouped = bundle.output.groupBy(_.indexInBundle).map{case (_, value) => value}.toList
      val bundleCount = inputGrouped.head.length

      val inputElements = inputGrouped
        .foldLeft(List(List(UnconfirmedTransactionMessage())))(
          (acc, b) => b.zipWithIndex.map{case (v, i) => acc(i) ++ List(v)}
        )

      val outputElements = outputGrouped
        .foldLeft(List(List(UnconfirmedTransactionMessage())))(
          (acc, b) => b.zipWithIndex.map{case (v, i) => acc(i) ++ List(v)}
        )


      val unconfirmedBundleInputs = inputElements.head
      val uncofirmedBundleOutputs = outputElements.head

      val bundleAmount = unconfirmedBundleInputs.foldLeft(0L)((acc, b) => acc + b.amount)
      val bundleHash = unconfirmedBundleInputs.head.bundleHash
      val bundleSize = unconfirmedBundleInputs.head.maxIndexInBundle + 1

      val unconfirmedBundle = UnconfirmedBundle(
        hash = bundleHash,
        amount = bundleAmount,
        input = unconfirmedBundleInputs,
        output = uncofirmedBundleOutputs,
        size = bundleSize
      )

      out.collect(unconfirmedBundle)

      val rangeList = List.range(0, bundleCount-1)

      val elements = rangeList.map(i => ReattachmentBundle(
        hash = bundleHash,
        amount = bundleAmount,
        input = inputElements(i),
        output = outputElements(i),
        size = bundleSize
      ))

      elements.foreach(out.collect)
    } else {
      out.collect(bundle)
    }
  }
}
