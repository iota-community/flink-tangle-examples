package org.iota

import org.iota.tangle.stream.messages.transactionMessages.UnconfirmedTransactionMessage

trait TransactionBundle {
  val hash: String
  val amount: Long
  val input: List[UnconfirmedTransactionMessage]
  val output: List[UnconfirmedTransactionMessage]
  val size: Int
}
