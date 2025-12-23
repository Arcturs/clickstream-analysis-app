package ru.spb.itmo.pirsbd.asashina.generator.dto

data class Message(
    var events: List<ClickEvent>? = null,
)