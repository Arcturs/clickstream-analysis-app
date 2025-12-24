package ru.spb.pirsbd.asashina.common.dto

import java.util.UUID

data class ClickEvent(
    var id: String = UUID.randomUUID().toString(),
    var type: String? = null,
    var createdAt: String? = null,
    var receivedAt: String? = null,
    var sessionId: String? = null,
    var ip: String? = null,
    var userId: Int? = null,
    var url: String? = null,
    var referrer: String? = null,
    var deviceType: String? = null,
    var userAgent: String? = null,
    var payload: ClickEventPayload? = null,
)