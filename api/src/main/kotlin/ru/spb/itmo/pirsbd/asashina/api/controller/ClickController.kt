package ru.spb.itmo.pirsbd.asashina.api.controller

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.spb.itmo.pirsbd.asashina.api.model.request.ClickEventRequest
import ru.spb.itmo.pirsbd.asashina.api.service.ClickEventService

@RestController
@RequestMapping("/clicks")
@Tag(name = "Click Controller")
class ClickController(
    private val service: ClickEventService,
) {

    @PostMapping
    @Operation(summary = "Создание кликового события", operationId = "createClick")
    fun createClickEvent(@RequestBody request: ClickEventRequest) =
        ResponseEntity.status(201).body(service.save(request))

}