package net.manub.embeddedkafka

import scala.language.postfixOps

class EmbeddedKafkaCustomConfigSpec extends EmbeddedKafkaSpecSupport with EmbeddedKafka {

  "the custom config" should {
    "should allow pass additional producer parameters" in {
      val customProducerConfig = Map("" -> "")
      implicit val customKafkaConfig = EmbeddedKafkaConfig(customProducerProperties = customProducerConfig)
      val bigMessage = generateMessageOfLength(2000000)
      val topic = "big-message-topic"

      withRunningKafka {
        publishStringMessageToKafka(topic, bigMessage)
        consumeFirstStringMessageFrom(topic) shouldBe bigMessage
      }
    }
  }

  def generateMessageOfLength(length: Int): String = Stream.continually(util.Random.nextPrintableChar) take length mkString
}