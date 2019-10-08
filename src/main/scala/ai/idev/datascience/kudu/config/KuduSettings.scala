
package ai.idev.datascience.kudu.config

import com.datamountaineer.kcql.{Kcql, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ThrowErrorPolicy}

case class KuduSettings(kcql: List[Kcql],
                        topicTables: Map[String, String],
                        allowAutoCreate: Map[String, Boolean],
                        allowAutoEvolve: Map[String, Boolean],
                        fieldsMap: Map[String, Map[String, String]],
                        ignoreFields: Map[String, Set[String]],
                        writeModeMap: Map[String, WriteModeEnum],
                        errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                        maxRetries: Int = KuduConfigConstants.NBR_OF_RETIRES_DEFAULT,
                        schemaRegistryUrl: String,
                        writeFlushMode: WriteFlushMode.WriteFlushMode,
                        mutationBufferSpace: Int
                       )

object KuduSettings {

  def apply(config: KuduConfig): KuduSettings = {

    val kcql = config.getKCQL
    val errorPolicy = config.getErrorPolicy
    val maxRetries = config.getNumberRetries
    val autoCreate = config.getAutoCreate()
    val autoEvolve = config.getAutoEvolve()
    val schemaRegUrl = config.getSchemaRegistryUrl
    val fieldsMap = config.getFieldsMap()
    val ignoreFields = config.getIgnoreFieldsMap()
    val writeModeMap = config.getWriteMode()
    val topicTables = config.getTableTopic()
    val writeFlushMode = config.getWriteFlushMode()
    val mutationBufferSpace = config.getInt(KuduConfigConstants.MUTATION_BUFFER_SPACE)

    new KuduSettings(kcql = kcql.toList,
      topicTables = topicTables,
      allowAutoCreate = autoCreate,
      allowAutoEvolve = autoEvolve,
      fieldsMap = fieldsMap,
      ignoreFields = ignoreFields,
      writeModeMap = writeModeMap,
      errorPolicy = errorPolicy,
      maxRetries = maxRetries,
      schemaRegistryUrl = schemaRegUrl,
      writeFlushMode = writeFlushMode,
      mutationBufferSpace = mutationBufferSpace
     )
  }
}


