package ru.spb.itmo.pirsbd.asashina.api.service

import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEvent
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEventKey
import ru.spb.itmo.pirsbd.asashina.api.model.request.ClickEventRequest
import ru.spb.itmo.pirsbd.asashina.api.repository.ClickEventRepository
import java.time.LocalDateTime
import java.util.UUID

@Service
class ClickEventService(
    private val clickEventRepository: ClickEventRepository
) {

    @Transactional
    fun save(eventRequest: ClickEventRequest): ClickEvent {
        val clickEvent = convertToEntity(eventRequest)
        return clickEventRepository.save(clickEvent)
    }

    private fun convertToEntity(request: ClickEventRequest): ClickEvent {
        val createdAt = request.createdAt?.let { LocalDateTime.parse(it) } ?: LocalDateTime.now()
        val receivedAt = request.receivedAt?.let { LocalDateTime.parse(it) } ?: LocalDateTime.now()

        return ClickEvent(
            key = ClickEventKey(
                userId = request.userId,
                createdAt = createdAt,
                id = UUID.randomUUID().toString()
            ),
            type = request.type,
            receivedAt = receivedAt,
            sessionId = request.sessionId,
            ip = request.ip,
            url = request.url,
            referrer = request.referrer,
            deviceType = request.deviceType,
            userAgent = request.userAgent,
            eventTitle = request.payload?.eventTitle,
            elementId = request.payload?.elementId,
            x = request.payload?.x,
            y = request.payload?.y,
            elementText = request.payload?.elementText,
            elementClass = request.payload?.elementClass,
            pageTitle = request.payload?.pageTitle,
            viewportWidth = request.payload?.viewportWidth,
            viewportHeight = request.payload?.viewportHeight,
            scrollPosition = request.payload?.scrollPosition,
            timestampOffset = request.payload?.timestampOffset,
            metadata = request.payload?.metadata?.mapValues { it.value.toString() }
        )
    }

}