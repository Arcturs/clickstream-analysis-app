package ru.spb.itmo.pirsbd.asashina.generator.producer

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Captor
import org.mockito.Mock
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import ru.spb.itmo.pirsbd.asashina.generator.model.ClickEvent
import java.time.LocalDateTime
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeoutException

@ExtendWith(MockitoExtension::class)
class KafkaEventProducerTest {

    @Mock
    private lateinit var kafkaTemplate: KafkaTemplate<String, ClickEvent>

    @Captor
    private lateinit var topicCaptor: ArgumentCaptor<String>

    @Captor
    private lateinit var messageCaptor: ArgumentCaptor<ClickEvent>

    private lateinit var kafkaEventProducer: KafkaEventProducer
    private lateinit var testEvent: ClickEvent

    @BeforeEach
    fun setUp() {
        kafkaEventProducer = KafkaEventProducer("test-topic", kafkaTemplate)

        testEvent = ClickEvent(
            id = "test-id",
            type = "CLICK",
            createdAt = LocalDateTime.now(),
            receivedAt = LocalDateTime.now(),
            sessionId = "session_123",
            ip = "192.168.1.1",
            userId = 123,
            url = "/test",
            deviceType = "DESKTOP",
            userAgent = "Test Agent"
        )
    }

    @Test
    fun `sendMessage should call kafkaTemplate send with correct topic and message`() {
        val future = CompletableFuture<SendResult<String, ClickEvent>>()
        future.complete(mock())
        `when`(kafkaTemplate.send(anyString(), any(ClickEvent::class.java)))
            .thenReturn(future)

        val result = kafkaEventProducer.sendMessage(testEvent)

        verify(kafkaTemplate).send(topicCaptor.capture(), messageCaptor.capture())

        assertThat(topicCaptor.value).isEqualTo("test-topic")
        assertThat(messageCaptor.value).isEqualTo(testEvent)
        assertThat(result).isTrue()
    }

    @Test
    fun `sendMessage should return false when kafka send fails`() {
        val future = CompletableFuture<SendResult<String, ClickEvent>>()
        future.completeExceptionally(TimeoutException("Kafka timeout"))
        `when`(kafkaTemplate.send(anyString(), any(ClickEvent::class.java)))
            .thenReturn(future)

        val result = kafkaEventProducer.sendMessage(testEvent)

        assertThat(result).isFalse()
    }

    @Test
    fun `sendMessage should handle null exception in handle method`() {
        val future = CompletableFuture<SendResult<String, ClickEvent>>()
        future.complete(null)
        `when`(kafkaTemplate.send(anyString(), any(ClickEvent::class.java)))
            .thenReturn(future)

        val result = kafkaEventProducer.sendMessage(testEvent)

        assertThat(result).isTrue()
    }
}