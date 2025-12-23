package ru.spb.itmo.pirsbd.asashina.generator

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import

@Import(TestcontainersConfiguration::class)
@SpringBootTest
class GeneratorApplicationTests {

    @Test
    fun contextLoads() {
    }

}
