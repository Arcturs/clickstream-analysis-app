package ru.spb.itmo.pirsbd.asashina.generator.scheduler

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import ru.spb.itmo.pirsbd.asashina.generator.generator.DataGenerator
import ru.spb.itmo.pirsbd.asashina.generator.producer.KafkaEventProducer

@Async
@Component
class EventGeneratorScheduler(
    private val generator: DataGenerator,
    private val producer: KafkaEventProducer,
    @Value("\${scheduler.batchSize}") private val batchSize: Int
) {

    @Scheduled(fixedDelay = 500)
    fun sendEvents() {
        repeat (batchSize) {
            producer.sendMessage(generator.generateSingleEvent())
        }
        log.info("Сгенерировано $batchSize сообщений и отправлено в кафку")
    }

    private companion object {
        val log: Logger = LoggerFactory.getLogger(EventGeneratorScheduler::class.java)
    }

}