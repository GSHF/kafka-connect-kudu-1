package ai.idev.datascience.kudu.parser

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kudu.client.{KuduTable, Upsert}

class AvroEventParser extends StrictLogging with EventParser {
  def parseValue(sinkRecord: SinkRecord, kuduTable: KuduTable): Upsert = {

    val recordFields = sinkRecord.valueSchema().fields()
    val kuduColNames = kuduTable.getSchema.getColumns.map(c => c.getName)
    val upsert = kuduTable.newUpsert()
    val row = upsert.getRow
  }
}
