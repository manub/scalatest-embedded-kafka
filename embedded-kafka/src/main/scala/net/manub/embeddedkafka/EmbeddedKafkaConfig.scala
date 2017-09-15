package net.manub.embeddedkafka

trait EmbeddedKafkaConfig {
  val kafkaPort: Int
  val zooKeeperPort: Int
  val customBrokerProperties: Map[String, String]
  val customProducerProperties: Map[String, String]
  val customConsumerProperties: Map[String, String]
}

case class EmbeddedKafkaConfigImpl(
                                    kafkaPort: Int,
                                    zooKeeperPort: Int,
                                    customBrokerProperties: Map[String, String],
                                    customProducerProperties: Map[String, String],
                                    customConsumerProperties: Map[String, String]
                                  ) extends EmbeddedKafkaConfig

object EmbeddedKafkaConfig {
  implicit val defaultConfig: EmbeddedKafkaConfig = apply()

  def apply(
             kafkaPort: Int = 6001,
             zooKeeperPort: Int = 6000,
             customBrokerProperties: Map[String, String] = Map.empty,
             customProducerProperties: Map[String, String] = Map.empty,
             customConsumerProperties: Map[String, String] = Map.empty
           ) : EmbeddedKafkaConfig =
    EmbeddedKafkaConfigImpl(kafkaPort, zooKeeperPort, customBrokerProperties, customProducerProperties, customConsumerProperties)
}
