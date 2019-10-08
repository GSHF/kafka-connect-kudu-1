package ai.idev.datascience.kudu.sink

import java.util

import ai.idev.datascience.kudu.KuduSinkClient
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import ai.idev.datascience.kudu.config.{KuduConfig, KuduConfigConstants, KuduSettings}
import com.datamountaineer.streamreactor.connect.utils.{JarManifest, ProgressCounter}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._

class KuduSinkTask extends SinkTask with StrictLogging {
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private var writer: Option[KuduSinkClient] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    // logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/kudu-ascii.txt")).mkString + s" $version")
    logger.info(manifest.printManifest())

    val conf = if (context.configs().isEmpty) props else context.configs()
    KuduConfig.config.parse(conf)
    val sinkConfig = new KuduConfig(conf)
    enableProgress = sinkConfig.getBoolean(KuduConfigConstants.PROGRESS_COUNTER_ENABLED)
    val settings = KuduSettings(sinkConfig)

    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(KuduConfigConstants.ERROR_RETRY_INTERVAL).toLong)
    }

    writer = Some(KuduSinkClient(sinkConfig, settings))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.toVector
    writer.foreach(w => w.write(records.toSeq))

    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  /**
    * Clean up writer
    **/
  override def stop(): Unit = {
    logger.info("Stopping Kudu sink.")
    writer.foreach(w => w.close())
    progressCounter.empty
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.flush())
  }

  override def version: String = manifest.version()
}
