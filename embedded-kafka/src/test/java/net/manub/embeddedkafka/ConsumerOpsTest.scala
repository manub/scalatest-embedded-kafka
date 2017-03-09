package net.manub.embeddedkafka

import net.manub.embeddedkafka.ConsumerExtensions._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class ConsumerOpsTest extends EmbeddedKafkaSpecSupport with MockitoSugar {

  "ConsumeLazily " should {
    "retry to get messages with the configured maximum number of attempts when poll fails" in {
      val consumer: KafkaConsumer[String, String] = mock[KafkaConsumer[String, String]]
      val consumerRecords = new ConsumerRecords[String, String](mapAsJavaMap(Map.empty))

      when(consumer.poll(1)).thenReturn(consumerRecords)

      val maximumAttempts = 2
      consumer.consumeLazily("topic", maximumAttempts, 1)

      verify(consumer, times(maximumAttempts)).poll(1)
    }

    "not retry to get messages with the configured maximum number of attempts when poll succeeds" in {
      val consumer: KafkaConsumer[String, String] = mock[KafkaConsumer[String, String]]
      val consumerRecord: ConsumerRecord[String, String] = mock[ConsumerRecord[String, String]]
      val consumerRecords = new ConsumerRecords[String, String](
        mapAsJavaMap(Map[TopicPartition, java.util.List[ConsumerRecord[String, String]]](new TopicPartition("topic", 1) -> List(consumerRecord).asJava))
      )

      when(consumer.poll(1)).thenReturn(consumerRecords)

      consumer.consumeLazily("topic", 1, 1)

      verify(consumer).poll(1)
    }

    "poll to get messages with the configured poll timeout" in {
      val consumer: KafkaConsumer[String, String] = mock[KafkaConsumer[String, String]]
      val consumerRecords = new ConsumerRecords[String, String](mapAsJavaMap(Map.empty))

      val pollTimeout = 10
      when(consumer.poll(pollTimeout)).thenReturn(consumerRecords)

      consumer.consumeLazily("topic", 1, pollTimeout)

      verify(consumer).poll(pollTimeout)
    }
  }

}
