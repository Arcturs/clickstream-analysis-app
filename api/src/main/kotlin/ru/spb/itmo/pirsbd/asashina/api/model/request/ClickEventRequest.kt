package ru.spb.itmo.pirsbd.asashina.api.model.request

data class ClickEventRequest(
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
    var payload: ClickEventPayloadRequest? = null
)

data class ClickEventPayloadRequest(
    var eventTitle: String? = null,
    var elementId: String? = null,
    var x: Int? = null,
    var y: Int? = null,
    var elementText: String? = null,
    var elementClass: String? = null,
    var pageTitle: String? = null,
    var viewportWidth: Int? = null,
    var viewportHeight: Int? = null,
    var scrollPosition: Double? = null,
    var timestampOffset: Long? = null,
    var metadata: Map<String, Any>? = null
)