package ai.idev.datascience.kudu.config

object WriteFlushMode extends Enumeration {
  type WriteFlushMode = Value
  val SYNC, BATCH_BACKGROUND, BATCH_SYNC = Value
}