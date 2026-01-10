package ru.spb.itmo.pirsbd.asashina.api.service

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.junit.jupiter.MockitoExtension
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEvent
import ru.spb.itmo.pirsbd.asashina.api.model.request.ClickEventPayloadRequest
import ru.spb.itmo.pirsbd.asashina.api.model.request.ClickEventRequest
import ru.spb.itmo.pirsbd.asashina.api.repository.ClickEventRepository
import java.time.LocalDateTime
import java.util.*

@ExtendWith(MockitoExtension::class)
class ClickEventServiceTest {

    @Mock
    private lateinit var clickEventRepository: ClickEventRepository

    @InjectMocks
    private lateinit var clickEventService: ClickEventService

    @Captor
    private lateinit var clickEventCaptor: ArgumentCaptor<ClickEvent>

    private lateinit var testRequest: ClickEventRequest
    private lateinit var testPayload: ClickEventPayloadRequest
    private val testTime = "2026-01-15T10:30:00"

    @BeforeEach
    fun setUp() {
        testPayload = ClickEventPayloadRequest(
            eventTitle = "Test Click",
            elementId = "btn-submit",
            x = 100,
            y = 200,
            elementText = "Submit",
            elementClass = "btn-primary",
            pageTitle = "Test Page",
            viewportWidth = 1920,
            viewportHeight = 1080,
            scrollPosition = 150.5,
            timestampOffset = 123456789L,
            metadata = mapOf("browser" to "Chrome", "version" to "120")
        )

        testRequest = ClickEventRequest(
            type = "click",
            createdAt = testTime,
            receivedAt = testTime,
            sessionId = "session-123",
            ip = "192.168.1.1",
            userId = 123,
            url = "https://example.com",
            referrer = "https://google.com",
            deviceType = "desktop",
            userAgent = "Mozilla/5.0",
            payload = testPayload
        )
    }

    @Test
    fun `save should convert request to entity and call repository`() {
        val savedEvent = ClickEvent()
        `when`(clickEventRepository.save(any(ClickEvent::class.java)))
            .thenReturn(savedEvent)

        val result = clickEventService.save(testRequest)

        verify(clickEventRepository).save(clickEventCaptor.capture())
        val capturedEvent = clickEventCaptor.value

        assertNotNull(capturedEvent.key)
        assertEquals(123, capturedEvent.key?.userId)
        assertEquals(LocalDateTime.parse(testTime), capturedEvent.key?.createdAt)
        assertNotNull(capturedEvent.key?.id)
        assertEquals("click", capturedEvent.type)
        assertEquals("session-123", capturedEvent.sessionId)
        assertEquals("192.168.1.1", capturedEvent.ip)
        assertEquals("https://example.com", capturedEvent.url)
        assertEquals("Test Click", capturedEvent.eventTitle)
        assertEquals(100, capturedEvent.x)
        assertEquals(200, capturedEvent.y)
        assertEquals(mapOf("browser" to "Chrome", "version" to "120"), capturedEvent.metadata)
        assertEquals(savedEvent, result)
    }

    @Test
    fun `save should handle null payload gracefully`() {
        testRequest.payload = null
        `when`(clickEventRepository.save(any(ClickEvent::class.java)))
            .thenReturn(ClickEvent())

        val result = clickEventService.save(testRequest)

        verify(clickEventRepository).save(clickEventCaptor.capture())
        val capturedEvent = clickEventCaptor.value

        assertNull(capturedEvent.eventTitle)
        assertNull(capturedEvent.elementId)
        assertNull(capturedEvent.x)
        assertNull(capturedEvent.y)
        assertNull(capturedEvent.metadata)
        assertNotNull(result)
    }

    @Test
    fun `save should convert metadata values to string`() {
        testPayload.metadata = mapOf(
            "number" to 123,
            "boolean" to true,
            "double" to 123.45
        )
        `when`(clickEventRepository.save(any(ClickEvent::class.java)))
            .thenReturn(ClickEvent())

        clickEventService.save(testRequest)

        verify(clickEventRepository).save(clickEventCaptor.capture())
        val capturedEvent = clickEventCaptor.value

        assertEquals("123", capturedEvent.metadata?.get("number"))
        assertEquals("true", capturedEvent.metadata?.get("boolean"))
        assertEquals("123.45", capturedEvent.metadata?.get("double"))
    }
}