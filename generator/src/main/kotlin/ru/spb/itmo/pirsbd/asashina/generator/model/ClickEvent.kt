package ru.spb.itmo.pirsbd.asashina.generator.model

import java.time.LocalDateTime
import java.util.UUID

data class ClickEvent(
    var id: String = UUID.randomUUID().toString(),
    var type: String? = null,
    var createdAt: LocalDateTime? = null,
    var receivedAt: LocalDateTime? = null,
    var sessionId: String? = null,
    var ip: String? = null,
    var userId: Int? = null,
    var url: String? = null,
    var referrer: String? = null,
    var deviceType: String? = null,
    var userAgent: String? = null,
    var payload: ClickEventPayload? = null,
)