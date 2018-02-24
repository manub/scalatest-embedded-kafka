package net.manub.embeddedkafka

import kafka.server.KafkaServer
import org.apache.zookeeper.server.ServerCnxnFactory

import scala.reflect.io.Directory

sealed trait EmbeddedServer {

  def stop(clearLogs: Boolean): Unit
}

case class EmbeddedZ(factory: ServerCnxnFactory,
                     logsDirs: Directory)(
                      implicit config: EmbeddedKafkaConfig) extends EmbeddedServer {

  override def stop(clearLogs: Boolean) = {
    factory.shutdown()
    if (clearLogs) logsDirs.deleteRecursively()
  }
}

case class EmbeddedK(factory: Option[EmbeddedZ],
                     broker: KafkaServer,
                     logsDirs: Directory)(
                      implicit config: EmbeddedKafkaConfig) extends EmbeddedServer {

  override def stop(clearLogs: Boolean) = {
    broker.shutdown()
    broker.awaitShutdown()

    factory.foreach(_.stop(clearLogs))

    if (clearLogs) logsDirs.deleteRecursively()
  }
}

object EmbeddedK {
  def apply(broker: KafkaServer, logsDirs: Directory)(
    implicit config: EmbeddedKafkaConfig): EmbeddedK =
    EmbeddedK(None, broker, logsDirs)
}
