package net.manub.embeddedkafka

case class EmbeddedKafkaConfig(kafkaPort: Int = 9092,
                               zooKeeperPort: Int = 6000,
                               customBrokerProperties: Map[String, String] = Map.empty,
                               customProducerProperties: Map[String, String] = Map.empty,
                               customConsumerProperties: Map[String, String] = Map.empty)

object EmbeddedKafkaConfig {
  implicit val defaultConfig = EmbeddedKafkaConfig()
}
