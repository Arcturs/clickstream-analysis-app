package ru.spb.itmo.pirsbd.asashina.api.controller

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.spb.itmo.pirsbd.asashina.api.repository.ClickRepository
import ru.spb.pirsbd.asashina.common.dto.ClickEvent

@RestController
@RequestMapping("/clicks")
@Tag(name = "Click Controller")
class ClickController(
    private val repository: ClickRepository
) {

    @PostMapping
    @Operation(summary = "Создание кликового события", operationId = "createClick")
    fun createClickEvent(@RequestBody request: ClickEvent) =
        ResponseEntity.status(201).body(repository.save(request))

}