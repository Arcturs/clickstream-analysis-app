package ru.spb.itmo.pirsbd.asashina.generator.producer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import ru.spb.pirsbd.asashina.common.dto.ClickEvent

@Component
class KafkaEventProducer(
    @Value("\${spring.kafka.topic}") private val topic: String,
    private val eventKafkaTemplate: KafkaTemplate<String, ClickEvent>
) {

    fun sendMessage(message: ClickEvent): Boolean {
        return eventKafkaTemplate.send(topic, message)
            .handle { _, exception: Throwable? ->
                if (exception != null) {
                    log.warn("Произошла ошибка при попытке отправить сообщение в топик", exception)
                    return@handle false
                }
                true
            }
            .join()
    }

    private companion object {
        val log: Logger = LoggerFactory.getLogger(KafkaEventProducer::class.java)
    }

}