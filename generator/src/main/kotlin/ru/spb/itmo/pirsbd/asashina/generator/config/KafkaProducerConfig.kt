package ru.spb.itmo.pirsbd.asashina.generator.config

import org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JacksonJsonSerializer
import ru.spb.pirsbd.asashina.common.dto.ClickEvent
import kotlin.Any
import kotlin.String

@Configuration
class KafkaProducerConfig(
    @Value("\${spring.kafka.topic}") private val topic: String,
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String
) {

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, ClickEvent?> = KafkaTemplate(producerFactory())

    @Bean
    fun producerFactory(): ProducerFactory<String, ClickEvent?> =
        DefaultKafkaProducerFactory(
            mapOf<String, Any>(
                BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                BATCH_SIZE_CONFIG to "5000"
            ),
            StringSerializer(),
            JacksonJsonSerializer<ClickEvent?>().apply {
                isAddTypeInfo = false
            }
        )

}