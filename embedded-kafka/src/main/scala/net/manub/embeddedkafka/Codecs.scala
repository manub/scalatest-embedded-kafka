package net.manub.embeddedkafka

import kafka.serializer._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization._

/** useful encoders/serializers and decoders/deserializers **/
object Codecs {
  implicit val stringEncoder: Encoder[String] = new StringEncoder()
  implicit val nullEncoder: Encoder[Array[Byte]] = new DefaultEncoder()
  implicit val stringSerializer: Serializer[String] = new StringSerializer()
  implicit val nullSerializer: Serializer[Array[Byte]] =
    new ByteArraySerializer()

  implicit val stringDecoder: Decoder[String] = new StringDecoder()
  implicit val nullDecoder: Decoder[Array[Byte]] = new DefaultDecoder()
  implicit val stringDeserializer: Deserializer[String] =
    new StringDeserializer()
  implicit val nullDeserializer: Deserializer[Array[Byte]] =
    new ByteArrayDeserializer()

  implicit val stringKeyValueCrDecoder
    : ConsumerRecord[String, String] => (String, String) =
    cr => (cr.key(), cr.value)
  implicit val stringValueCrDecoder: ConsumerRecord[String, String] => String =
    _.value()

  implicit val keyNullValueCrDecoder
    : ConsumerRecord[String, Array[Byte]] => (String, Array[Byte]) =
    cr => (cr.key(), cr.value)
  implicit val nullValueCrDecoder
    : ConsumerRecord[String, Array[Byte]] => Array[Byte] = _.value()
}
