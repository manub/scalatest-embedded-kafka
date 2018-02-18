package net.manub.embeddedkafka

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import net.manub.embeddedkafka.EmbeddedKafka._

import scala.collection.JavaConverters._

class EmbeddedKafkaObjectSpec extends EmbeddedKafkaSpecSupport {

  val consumerPollTimeout = 5000

  "the EmbeddedKafka object" when {
    "invoking the start and stop methods" should {
      "start and stop Kafka and Zookeeper on the default ports" in {
        EmbeddedKafka.start()

        kafkaIsAvailable()
        zookeeperIsAvailable()

        EmbeddedKafka.stop()

        kafkaIsNotAvailable()
        zookeeperIsNotAvailable()
      }

      "start and stop Kafka and Zookeeper on different specified ports using an implicit configuration" in {
        implicit val config =
          EmbeddedKafkaConfig(kafkaPort = 12345, zooKeeperPort = 54321)
        EmbeddedKafka.start()

        kafkaIsAvailable(12345)
        zookeeperIsAvailable(54321)

        EmbeddedKafka.stop()
      }

      "multiple EmbeddedKafka can run in parallel" in {
        val someConfig = EmbeddedKafkaConfig(kafkaPort = 12345, zooKeeperPort = 32111)
        EmbeddedKafka.start()(someConfig)

        val someOtherConfig = EmbeddedKafkaConfig(kafkaPort = 23456, zooKeeperPort = 43211)
        EmbeddedKafka.start()(someOtherConfig)

        val topic = "publish_test_topic_1"
        val someMessage = "hello world!"
        val someOtherMessage = "another message!"

        val serializer = new StringSerializer
        val deserializer = new StringDeserializer

        publishToKafka(topic, someMessage)(someConfig, serializer)
        publishToKafka(topic, someOtherMessage)(someOtherConfig, serializer)

        val consumer = kafkaConsumer(someConfig, deserializer, deserializer)
        consumer.subscribe(List(topic).asJava)

        val records = consumer.poll(consumerPollTimeout)
        records.count shouldBe 1

        val record = records.iterator().next
        record.value shouldBe someMessage

        // second

        val anotherConsumer = kafkaConsumer(someOtherConfig, deserializer, deserializer)
        anotherConsumer.subscribe(List(topic).asJava)

        val moreRecords = anotherConsumer.poll(consumerPollTimeout)
        moreRecords.count shouldBe 1

        val someOtherRecord = moreRecords.iterator().next
        someOtherRecord.value shouldBe someOtherMessage

        EmbeddedKafka.stop()
        EmbeddedKafka.stop()
      }
    }

    "invoking the isRunnning method" should {
      "return whether both Kafka and Zookeeper are running" in {
        EmbeddedKafka.start()
        EmbeddedKafka.isRunning shouldBe true
        EmbeddedKafka.stop()
        EmbeddedKafka.isRunning shouldBe false
      }
    }
  }
}
