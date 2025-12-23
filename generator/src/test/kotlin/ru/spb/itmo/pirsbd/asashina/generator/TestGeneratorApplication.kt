package ru.spb.itmo.pirsbd.asashina.generator

import org.springframework.boot.fromApplication
import org.springframework.boot.with


fun main(args: Array<String>) {
    fromApplication<GeneratorApplication>().with(TestcontainersConfiguration::class).run(*args)
}
