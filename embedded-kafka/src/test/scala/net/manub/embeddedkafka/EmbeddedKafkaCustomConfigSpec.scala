package net.manub.embeddedkafka

import scala.language.postfixOps

class EmbeddedKafkaCustomConfigSpec extends EmbeddedKafkaSpecSupport with EmbeddedKafka {
  val TWO_MEGABYTES = 2097152
  val THREE_MEGABYTES = 3145728

  "the custom config" should {
    "should allow pass additional producer parameters" in {
      val customBrokerConfig = Map(
        "replica.fetch.max.bytes" -> s"$THREE_MEGABYTES",
        "message.max.bytes" -> s"$THREE_MEGABYTES")

      val customProducerConfig = Map("max.request.size" -> s"$THREE_MEGABYTES")
      val customConsumerConfig = Map("max.partition.fetch.bytes" -> s"$THREE_MEGABYTES")

      implicit val customKafkaConfig = EmbeddedKafkaConfig(
        customBrokerProperties = customBrokerConfig,
        customProducerProperties = customProducerConfig,
        customConsumerProperties = customConsumerConfig)

      val bigMessage = generateMessageOfLength(TWO_MEGABYTES)
      val topic = "big-message-topic"

      withRunningKafka {
        publishStringMessageToKafka(topic, bigMessage)
        consumeFirstStringMessageFrom(topic) shouldBe bigMessage
      }
    }
  }

  def generateMessageOfLength(length: Int): String = Stream.continually(util.Random.nextPrintableChar) take length mkString
}