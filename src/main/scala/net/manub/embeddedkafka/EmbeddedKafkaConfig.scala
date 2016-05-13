package net.manub.embeddedkafka

case class EmbeddedKafkaConfig(kafkaPort: Int = 6001, zooKeeperPort: Int = 6000, customBrokerProperties: Map[String,String] = Map())

object EmbeddedKafkaConfig {
  implicit val defaultConfig = EmbeddedKafkaConfig()
}