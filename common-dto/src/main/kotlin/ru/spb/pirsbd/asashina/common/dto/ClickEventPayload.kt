package ru.spb.pirsbd.asashina.common.dto

data class ClickEventPayload(
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