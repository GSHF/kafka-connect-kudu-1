package ai.idev.datascience.kudu.parser

import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kudu.client.{KuduTable, Upsert}

trait EventParser {
  def parseValue(sinkRecord: SinkRecord, table: KuduTable): Upsert
}
