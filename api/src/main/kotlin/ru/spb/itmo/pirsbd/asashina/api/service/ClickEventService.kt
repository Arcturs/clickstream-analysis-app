package ru.spb.itmo.pirsbd.asashina.api.service

import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEvent
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEventBySession
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEventBySessionKey
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEventByType
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEventByTypeKey
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEventKey
import ru.spb.itmo.pirsbd.asashina.api.model.request.ClickEventRequest
import ru.spb.itmo.pirsbd.asashina.api.repository.ClickEventBySessionRepository
import ru.spb.itmo.pirsbd.asashina.api.repository.ClickEventByTypeRepository
import ru.spb.itmo.pirsbd.asashina.api.repository.ClickEventRepository
import java.time.LocalDateTime
import java.util.UUID

@Service
class ClickEventService(
    private val clickEventRepository: ClickEventRepository,
    private val clickEventByTypeRepository: ClickEventByTypeRepository,
    private val clickEventBySessionRepository: ClickEventBySessionRepository
) {

    @Transactional
    fun save(eventRequest: ClickEventRequest): ClickEvent {
        val clickEvent = convertToEntity(eventRequest)
        val savedEvent = clickEventRepository.save(clickEvent)

        saveToAdditionalTables(savedEvent)

        return savedEvent
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

    private fun saveToAdditionalTables(event: ClickEvent) {
        clickEventBySessionRepository.save(
            ClickEventBySession(
                key = ClickEventBySessionKey(
                    sessionId = event.sessionId,
                    createdAt = event.key?.createdAt ?: LocalDateTime.now(),
                    id = event.key?.id ?: UUID.randomUUID().toString()
                ),
                userId = event.key?.userId,
                type = event.type,
                receivedAt = event.receivedAt,
                ip = event.ip,
                url = event.url,
                referrer = event.referrer,
                deviceType = event.deviceType,
                userAgent = event.userAgent,
                eventTitle = event.eventTitle
            )
        )

        event.type?.let { type ->
            clickEventByTypeRepository.save(
                ClickEventByType(
                    key = ClickEventByTypeKey(
                        type = type,
                        createdAt = event.key?.createdAt ?: LocalDateTime.now(),
                        userId = event.key?.userId,
                        id = event.key?.id ?: UUID.randomUUID().toString()
                    ),
                    sessionId = event.sessionId,
                    receivedAt = event.receivedAt,
                    ip = event.ip,
                    url = event.url,
                    referrer = event.referrer,
                    deviceType = event.deviceType,
                    userAgent = event.userAgent,
                    eventTitle = event.eventTitle
                )
            )
        }
    }

}