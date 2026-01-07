package ru.spb.itmo.pirsbd.asashina.generator.generator

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.DeviceType
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventType
import java.time.LocalDateTime

class DataGeneratorTest {

    private val dataGenerator = DataGenerator()

    @Test
    fun `generateSingleEvent should return valid ClickEvent`() {
        val event = dataGenerator.generateSingleEvent()

        assertAll(
            { assertThat(event).isNotNull },
            { assertThat(event.id).isNotEmpty },
            { assertThat(event.type).isIn("VIEW", "CLICK") },
            { assertThat(event.createdAt).isNotNull },
            { assertThat(event.receivedAt).isNotNull },
            { assertThat(event.sessionId).startsWith("session_") },
            { assertThat(event.ip).matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}") },
            { assertThat(event.userId).isBetween(0, 99) },
            { assertThat(event.url).isNotEmpty },
            { assertThat(event.deviceType).isIn(DeviceType.values().map { it.name }) },
            { assertThat(event.userAgent).isNotEmpty },
            { assertThat(event.payload).isNotNull }
        )
    }

    @Test
    fun `receivedAt should be after createdAt`() {
        val event = dataGenerator.generateSingleEvent()

        assertThat(event.receivedAt).isAfterOrEqualTo(event.createdAt)
    }

    @Test
    fun `sessionId should contain userId`() {
        val event = dataGenerator.generateSingleEvent()

        assertThat(event.sessionId).contains(event.userId.toString())
    }

    @RepeatedTest(10)
    fun `generateSingleEvent should generate different events`() {
        val events = (1..10).map { dataGenerator.generateSingleEvent() }

        val uniqueIds = events.map { it.id }.toSet()
        assertThat(uniqueIds).hasSize(10)
    }

    @Test
    fun `generated event should have valid dates within range`() {
        val event = dataGenerator.generateSingleEvent()
        val now = LocalDateTime.now()

        assertAll(
            { assertThat(event.createdAt).isBeforeOrEqualTo(now) },
            { assertThat(event.createdAt).isAfterOrEqualTo(now.minusDays(30)) }
        )
    }

    @Test
    fun `payload should be correctly generated for VIEW events`() {
        val event = dataGenerator.generateSingleEvent()

        if (event.type == "CLICK") {
            return
        }

        val payload = event.payload!!
        assertAll(
            { assertThat(payload.eventTitle).isNotNull },
            {
                val viewTitles = EventType.VIEW.let {
                    DataGenerator::class.java.getDeclaredField("EVENT_TYPE_TO_TITLES")
                        .apply { isAccessible = true }
                        .get(null) as Map<EventType, List<EventTitle>>
                }[EventType.VIEW]!!.map { it.name }
                assertThat(payload.eventTitle).isIn(viewTitles)
            },
            { assertThat(payload.elementId).isNotEmpty },
            { assertThat(payload.x).isBetween(0, 1919) },
            { assertThat(payload.y).isBetween(0, 1079) },
            { assertThat(payload.elementText).isNull() },
            { assertThat(payload.elementClass).isNull() },
            { assertThat(payload.pageTitle).isNotEmpty },
            { assertThat(payload.viewportWidth).isNotNull },
            { assertThat(payload.viewportHeight).isNotNull }
        )
    }

    @Test
    fun `payload should be correctly generated for CLICK events`() {
        val event = dataGenerator.generateSingleEvent()

        if (event.type == "VIEW") {
            return
        }

        val payload = event.payload!!
        assertAll(
            { assertThat(payload.eventTitle).isNotNull },
            {
                val clickTitles = EventType.CLICK.let {
                    DataGenerator::class.java.getDeclaredField("EVENT_TYPE_TO_TITLES")
                        .apply { isAccessible = true }
                        .get(null) as Map<EventType, List<EventTitle>>
                }[EventType.CLICK]!!.map { it.name }
                assertThat(payload.eventTitle).isIn(clickTitles)
            },
            { assertThat(payload.elementText).isNotEmpty },
            { assertThat(payload.elementClass).isNotEmpty }
        )
    }

    @Test
    fun `device-specific viewport dimensions should be correct`() {
        val event = dataGenerator.generateSingleEvent()
        val deviceType = DeviceType.valueOf(event.deviceType!!)
        val payload = event.payload!!

        val desktopWidths = listOf(1920, 1440, 1366, 1536)
        val mobileWidths = listOf(390, 414, 360, 375)
        val desktopHeights = listOf(1080, 900, 768, 864)
        val mobileHeights = listOf(844, 896, 812, 736)

        when (deviceType) {
            DeviceType.DESKTOP -> {
                assertThat(payload.viewportWidth).isIn(desktopWidths)
                assertThat(payload.viewportHeight).isIn(desktopHeights)
            }
            DeviceType.IOS, DeviceType.ANDROID -> {
                assertThat(payload.viewportWidth).isIn(mobileWidths)
                assertThat(payload.viewportHeight).isIn(mobileHeights)
            }
        }
    }

    @Test
    fun `metadata should be correctly generated for specific event types`() {
        repeat(50) {
            val event = dataGenerator.generateSingleEvent()
            val payload = event.payload!!
            val metadata = payload.metadata

            if (metadata != null) {
                assertAll(
                    { assertThat(metadata["ab_test_group"]).isIn("control", "variant_a", "variant_b") },
                    { assertThat(metadata["session_depth"] as Int).isBetween(1, 14) },
                    { assertThat(metadata["user_tier"]).isIn("free", "premium", "enterprise") },
                    { assertThat(metadata["referral_source"]).isIn("direct", "google", "facebook", "email", "organic") }
                )

                val eventTitle = EventTitle.valueOf(payload.eventTitle!!)
                when (eventTitle) {
                    EventTitle.ADD_TO_CART, EventTitle.PRODUCT_CLICK -> {
                        assertThat(metadata["product_id"]).isNotNull
                        assertThat(metadata["product_category"]).isIn("electronics", "clothing", "books", "home", "sports")
                    }
                    EventTitle.CHECKOUT_START, EventTitle.CHECKOUT_COMPLETE -> {
                        assertThat(metadata["order_id"]).isNotNull
                        assertThat(metadata["total_amount"]).isNotNull
                        assertThat(metadata["currency"]).isIn("USD", "EUR", "RUB", "GBP")
                    }
                    EventTitle.LOGIN_CLICK, EventTitle.SIGNUP_CLICK -> {
                        assertThat(metadata["auth_method"]).isIn("email", "google", "facebook", "apple")
                    }
                    else -> {}
                }
            }
        }
    }

    @Test
    fun `ip address should be valid`() {
        val event = dataGenerator.generateSingleEvent()
        val ipParts = event.ip!!.split(".")

        assertAll(
            { assertThat(ipParts).hasSize(4) },
            { assertThat(ipParts[0].toInt()).isBetween(1, 254) },
            { assertThat(ipParts[1].toInt()).isBetween(0, 254) },
            { assertThat(ipParts[2].toInt()).isBetween(0, 254) },
            { assertThat(ipParts[3].toInt()).isBetween(1, 254) }
        )
    }
}