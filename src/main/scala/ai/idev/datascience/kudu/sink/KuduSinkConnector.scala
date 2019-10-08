package ai.idev.datascience.kudu.sink

import java.util

import com.datamountaineer.streamreactor.connect.config.Helpers
import ai.idev.datascience.kudu.config.{KuduConfig, KuduConfigConstants}
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class KuduSinkConnector extends SinkConnector with StrictLogging {
  private val configDef = KuduConfig.config
  private var configProps: Option[util.Map[String, String]] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * States which SinkTask class to use
    **/
  override def taskClass(): Class[_ <: Task] = classOf[KuduSinkTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    **/
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(_ => configProps.get).toList
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting Kudu sink connector.")
    Helpers.checkInputTopics(KuduConfigConstants.KCQL, props.asScala.toMap)
    configProps = Some(props)
  }

  override def stop(): Unit = {}

  override def version(): String = manifest.version()

  override def config(): ConfigDef = configDef
}
