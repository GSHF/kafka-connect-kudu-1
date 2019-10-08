package ai.idev.datascience.kudu

import ai.idev.datascience.kudu.config.{KuduConfig, KuduConfigConstants, KuduSettings, WriteFlushMode}
import ai.idev.datascience.kudu.util.DbHandler
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client._
import org.json4s.JsonAST.JValue

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


case class SchemaMap(version: Int, schema: Schema)

object KuduSinkClient extends StrictLogging {

  def apply(config: KuduConfig, settings: KuduSettings): KuduSinkClient = {
    val kuduMaster = config.getString(KuduConfigConstants.KUDU_MASTER)
    logger.info(s"Connecting to Kudu Master at $kuduMaster")
    lazy val client = new KuduClient.KuduClientBuilder(kuduMaster.split(',').toList.asJava).build()
    new KuduSinkClient(client, settings)
  }
}

class KuduSinkClient(client: KuduClient, setting: KuduSettings) extends StrictLogging with KuduConverter
  with ErrorHandler with ConverterUtil {
  logger.info("Initialising Kudu writer")

  Try(DbHandler.createTables(setting, client)) match {
    case Success(_) =>
    case Failure(f) => logger.warn("Unable to create tables at startup! Tables will be created on delivery of the first record", f)
  }

  private val MUTATION_BUFFER_SPACE = setting.mutationBufferSpace
  private lazy val kuduTablesCache = collection.mutable.Map(DbHandler.buildTableCache(setting, client).toSeq: _*)
  private lazy val session = client.newSession()

  session.setFlushMode(setting.writeFlushMode match {
    case WriteFlushMode.SYNC =>
      FlushMode.AUTO_FLUSH_SYNC
    case WriteFlushMode.BATCH_SYNC =>
      FlushMode.MANUAL_FLUSH
    case WriteFlushMode.BATCH_BACKGROUND =>
      FlushMode.AUTO_FLUSH_BACKGROUND
  })

  session.setMutationBufferSpace(MUTATION_BUFFER_SPACE)

  //ignore duplicate in case of redelivery
  session.isIgnoreAllDuplicateRows

  //initialize error tracker
  initialize(setting.maxRetries, setting.errorPolicy)

  //schema cache
  val schemaCache: mutable.Map[String, SchemaMap] = mutable.Map.empty[String, SchemaMap]

  /**
    * Write SinkRecords to Kudu
    *
    * @param records A list of SinkRecords to write
    **/
  def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug(s"Received ${records.size} records.")

      //if error occurred rebuild cache in case of change on target tables
      if (errored()) {
        kuduTablesCache.empty
        DbHandler.buildTableCache(setting, client)
          .map({ case (topic, table) => kuduTablesCache.put(topic, table) })
      }
      applyInsert(records, session)
    }
  }

  /**
    * Per topic, build an new Kudu insert. Per insert build a Kudu row per SinkRecord.
    * Apply the insert per topic for the rows
    **/
  private def applyInsert(records: Seq[SinkRecord], session: KuduSession) = {
    def handleSinkRecord(record: SinkRecord): Upsert = {
      Option(record.valueSchema()) match {
        case None =>
          // try to take it as a string
          record.value() match {
            case _: java.util.Map[_, _] =>
              val converted = convert(record, setting.fieldsMap(record.topic), setting.ignoreFields(record.topic))
              val withDDLs = applyDDLs(converted)
              convertToKuduUpsert(withDDLs, kuduTablesCache(withDDLs.topic))
            case _ => sys.error("For schemaless record only String and Map types are supported")
          }
        case Some(schema: Schema) =>
          schema.`type`() match {
            case Schema.Type.STRING =>
              val converted = convertStringSchemaAndJson(record, setting.fieldsMap.getOrElse(record.topic, Map.empty[String, String]), setting.ignoreFields.getOrElse(record.topic, Set.empty))
              val withDDLs = applyDDLsFromJson(converted, record.topic)
              convertJsonToKuduUpsert(withDDLs, kuduTablesCache(record.topic))

            case Schema.Type.STRUCT =>
              val converted = convert(record, setting.fieldsMap(record.topic), setting.ignoreFields(record.topic))
              val withDDLs = applyDDLs(converted)
              convertToKuduUpsert(withDDLs, kuduTablesCache(withDDLs.topic))
            case other => sys.error(s"$other schema is not supported")
          }
      }
    }

    val t = Try({
      records.iterator
        .map(handleSinkRecord)
        .map(session.apply)
        .grouped(MUTATION_BUFFER_SPACE-1)
        .foreach(_ => flush())
    })
    handleTry(t)
    logger.debug(s"Written ${records.size}")
  }

  /**
    * Create the Kudu table if not already done and alter table if required
    *
    * @param record The sink record to create a table for
    * @return A KuduTable
    **/
  private def applyDDLs(record: SinkRecord): SinkRecord = {
    if (!kuduTablesCache.contains(record.topic())) {
      val mapping = setting.kcql.filter(f => f.getSource.equals(record.topic())).head
      val table = DbHandler.createTableFromSinkRecord(mapping, record.valueSchema(), client).get
      logger.info(s"Adding table ${mapping.getTarget} to the table cache")
      kuduTablesCache.put(mapping.getSource, table)
    } else {
      handleAlterTable(record)
    }
    record
  }

  private def applyDDLsFromJson(payload: JValue, topic: String): JValue = {
    if (!kuduTablesCache.contains(topic)) {
      val mapping = setting.kcql.filter(_.getSource.equals(topic)).head
      val table = DbHandler.createTableFromJsonPayload(mapping, payload, client, topic).get
      logger.info(s"Adding table ${mapping.getTarget} to the table cache")
      kuduTablesCache.put(mapping.getSource, table)
    } else {
      handleAlterTable(payload, topic)
    }
    payload
  }

  /**
    * Check alter table if schema has changed
    *
    * @param record The sinkRecord to check the schema for
    **/
  def handleAlterTable(record: SinkRecord): SinkRecord = {
    val topic = record.topic()
    val allowEvo = setting.allowAutoEvolve.getOrElse(topic, false)

    if (allowEvo) {
      val schema = record.valueSchema()
      val version = schema.version()
      val table = setting.topicTables(topic)

      val currentKcql = setting.kcql.filter(r => r.getTarget.trim == table)
      val currentKuduTableSchema = convertToKuduSchema(record, currentKcql.head)
      val currentColumnsSize = record.valueSchema().fields().size()

      val oldKuduTableSchema = kuduTablesCache(topic).getSchema
      val oldColumnsSize = oldKuduTableSchema.getColumns.size()

      //allow evolution
      val evolving = oldColumnsSize < currentColumnsSize

      //if table is allowed to evolve all the table
      if (evolving) {
        logger.info(s"Schema change detected for $topic mapped to table $table. Old schema columns size " +
          s"$oldKuduTableSchema new columns size $currentColumnsSize")
        val kuduTable = DbHandler.alterTable(table, oldKuduTableSchema, currentKuduTableSchema, client)

        kuduTablesCache.update(topic, kuduTable)
        schemaCache.update(topic, SchemaMap(version, schema))
      } else {
        schemaCache.update(topic, SchemaMap(version, schema))
      }
    }
    record
  }

  def handleAlterTable(payload: JValue, topic: String): JValue = {
    val fields = extractJSONFields(payload, topic)
    val cachedFields = getCacheJSONFields().getOrElse(topic, Map.empty)

    if (fields.nonEmpty && fields.keySet.diff(cachedFields.keySet).nonEmpty) {
      val table = setting.topicTables(topic)
      logger.info(s"Schema change detected for $topic mapped to table $table.")
      val kuduTable =  DbHandler.alterTable(table, cachedFields, fields, client)
      kuduTablesCache.update(topic, kuduTable)
    }
    payload
  }

  /**
    * Close the Kudu session and client
    **/
  def close(): Unit = {
    logger.info("Closing Kudu Session and Client")
    flush()
    if (!session.isClosed) session.close()
    client.shutdown()
  }

  /**
    * Force the session to flush it's buffers.
    *
    **/
  def flush(): Unit = {
    if (!session.isClosed) {

      //throw and let error policy handle it, don't want to throw RetriableException.
      //May want to die if error policy is Throw
      val errors : String = session.getFlushMode match {
        case FlushMode.AUTO_FLUSH_SYNC | FlushMode.MANUAL_FLUSH =>
          val flush = session.flush()
          if (flush != null) {
            flush.asScala
              .flatMap(r => Option(r))
              .withFilter(_.hasRowError)
              .map(_.getRowError.toString)
              .mkString(";")
          } else {
            ""
          }
        case FlushMode.AUTO_FLUSH_BACKGROUND =>
          session.getPendingErrors.getRowErrors
            .map(_.toString)
            .mkString(";")
      }
      if (errors.nonEmpty) {
        throw new RuntimeException(s"Failed to flush one or more changes:$errors")
      }
    }
  }
}
