package ru.spb.itmo.pirsbd.asashina.generator.generator

import org.springframework.stereotype.Component
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.DeviceType
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.DeviceType.ANDROID
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.DeviceType.DESKTOP
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.DeviceType.IOS
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.ADD_TO_CART
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.ARTICLE_VIEW
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.CART_VIEW
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.CATEGORY_VIEW
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.CHECKOUT_COMPLETE
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.CHECKOUT_START
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.CHECKOUT_VIEW
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.CONTACT_FORM_SUBMIT
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.FILTER_APPLY
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.HOMEPAGE_VIEW
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.LOGIN_CLICK
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.LOGIN_PAGE_VIEW
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.NEWSLETTER_SUBSCRIBE
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.PRODUCT_CLICK
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.PRODUCT_PAGE_VIEW
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.PROFILE_VIEW
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.REMOVE_FROM_CART
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.SEARCH_RESULTS_VIEW
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.SHARE_CLICK
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.SIGNUP_CLICK
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.SORT_CHANGE
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.UNKNOWN_EVENT
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.VIDEO_PLAY
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventTitle.WISHLIST_ADD
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventType
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventType.CLICK
import ru.spb.itmo.pirsbd.asashina.generator.dictionary.EventType.VIEW
import ru.spb.itmo.pirsbd.asashina.generator.model.ClickEvent
import ru.spb.itmo.pirsbd.asashina.generator.model.ClickEventPayload
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.random.Random

@Component
class DataGenerator {

    fun generateSingleEvent(): ClickEvent {
        val type = if (Random.nextBoolean()) VIEW else CLICK
        val deviceType = DeviceType.entries.random()

        val createdDate = randomDate()
        val receivedDate = createdDate.plusSeconds(Random.nextLong(0, 30))

        val userIdVal = Random.nextInt(100)
        val sessionIdVal = "session_${userIdVal}_${Random.nextInt(1000, 9999)}"

        val payload = generatePayload(type, deviceType)

        return ClickEvent(
            type = type.name,
            createdAt = createdDate,
            receivedAt = receivedDate,
            sessionId = sessionIdVal,
            ip = generateRandomIp(),
            userId = userIdVal,
            url = PAGES.random(),
            referrer = REFERRERS.random(),
            deviceType = deviceType.name,
            userAgent = DEVICE_TYPE_TO_USER_AGENTS[deviceType]?.random() ?: "",
            payload = payload
        )
    }

    private fun generatePayload(eventType: EventType, deviceType: DeviceType): ClickEventPayload {
        val eventTitle = EVENT_TYPE_TO_TITLES[eventType]?.random() ?: UNKNOWN_EVENT

        return ClickEventPayload(
            eventTitle = eventTitle.name,
            elementId = if (eventType == CLICK) ELEMENTS.random() else null,
            x = if (eventType == CLICK) Random.nextInt(0, 1920) else null,
            y = if (eventType == CLICK) Random.nextInt(0, 1080) else null,
            elementText = if (eventType == CLICK) generateElementText(eventTitle) else null,
            elementClass = if (eventType == CLICK) generateElementClass() else null,
            pageTitle = "Page Title ${Random.nextInt(1, 100)}",
            viewportWidth = (if (deviceType == DESKTOP) listOf(1920, 1440, 1366, 1536) else listOf(390, 414, 360, 375)).random(),
            viewportHeight = (if (deviceType == DESKTOP) listOf(1080, 900, 768, 864) else listOf(844, 896, 812, 736)).random(),
            scrollPosition = if (Random.nextDouble() > 0.3) Random.nextDouble(0.0, 1.0) else null,
            timestampOffset = if (Random.nextBoolean()) Random.nextLong(0, 3600000) else null,
            metadata = generateMetadata(eventTitle)
        )
    }

    private fun generateElementText(eventTitle: EventTitle): String {
        return when (eventTitle) {
            ADD_TO_CART -> "Add to Cart"
            CHECKOUT_START -> "Proceed to Checkout"
            LOGIN_CLICK -> "Sign In"
            SIGNUP_CLICK -> "Create Account"
            PRODUCT_CLICK -> "View Details"
            WISHLIST_ADD -> "Add to Wishlist"
            SHARE_CLICK -> "Share"
            else -> "Button ${Random.nextInt(1, 100)}"
        }
    }

    private fun generateElementClass() = "${BUTTON_TYPES.random()} ${BUTTON_SIZES.random()}"

    private fun generateMetadata(eventTitle: EventTitle): Map<String, Any>? {
        if (Random.nextDouble() > 0.7) {
            return null
        }

        val metadata = mutableMapOf<String, Any>()

        metadata["ab_test_group"] = listOf("control", "variant_a", "variant_b").random()
        metadata["session_depth"] = Random.nextInt(1, 15)
        metadata["user_tier"] = listOf("free", "premium", "enterprise").random()
        metadata["referral_source"] = listOf("direct", "google", "facebook", "email", "organic").random()

        when (eventTitle) {
            ADD_TO_CART, PRODUCT_CLICK -> {
                metadata["product_id"] = "prod_${Random.nextInt(1000, 9999)}"
                metadata["product_price"] = Random.nextDouble(10.0, 1000.0)
                metadata["product_category"] = listOf("electronics", "clothing", "books", "home", "sports").random()
                metadata["in_stock"] = Random.nextBoolean()
                if (Random.nextBoolean()) {
                    metadata["quantity"] = Random.nextInt(1, 5)
                }
            }
            CHECKOUT_START, CHECKOUT_COMPLETE -> {
                metadata["order_id"] = "ORD_${Random.nextInt(10000, 99999)}"
                metadata["total_amount"] = Random.nextDouble(50.0, 500.0)
                metadata["currency"] = listOf("USD", "EUR", "RUB", "GBP").random()
                metadata["payment_method"] = listOf("credit_card", "paypal", "apple_pay", "google_pay").random()
            }
            LOGIN_CLICK, SIGNUP_CLICK -> {
                metadata["auth_method"] = listOf("email", "google", "facebook", "apple").random()
                metadata["remember_me"] = Random.nextBoolean()
            }
            else -> {

            }
        }

        return metadata
    }

    private fun generateRandomIp(): String {
        return "${Random.nextInt(1, 255)}.${Random.nextInt(0, 255)}." +
                "${Random.nextInt(0, 255)}.${Random.nextInt(1, 255)}"
    }

    private fun randomDate(): LocalDateTime {
        val randomDays = (0..29).random()
        val randomHours = (0..23).random()
        val randomMinutes = (0..59).random()
        val randomSeconds = (0..59).random()

        return LocalDateTime.now()
            .minusDays(randomDays.toLong())
            .minusHours(randomHours.toLong())
            .minusMinutes(randomMinutes.toLong())
            .minusSeconds(randomSeconds.toLong())
    }

    private companion object {
        val EVENT_TYPE_TO_TITLES = mapOf(
            VIEW to listOf(
                HOMEPAGE_VIEW, PRODUCT_PAGE_VIEW, CATEGORY_VIEW,
                SEARCH_RESULTS_VIEW, CART_VIEW, CHECKOUT_VIEW,
                LOGIN_PAGE_VIEW, PROFILE_VIEW, ARTICLE_VIEW
            ),
            CLICK to listOf(
                ADD_TO_CART, REMOVE_FROM_CART, CHECKOUT_START,
                CHECKOUT_COMPLETE, LOGIN_CLICK, SIGNUP_CLICK,
                PRODUCT_CLICK, FILTER_APPLY, SORT_CHANGE,
                WISHLIST_ADD, SHARE_CLICK, VIDEO_PLAY,
                NEWSLETTER_SUBSCRIBE, CONTACT_FORM_SUBMIT
            )
        )
        val DEVICE_TYPE_TO_USER_AGENTS = mapOf(
            DESKTOP to listOf(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            IOS to listOf(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
                "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
            ),
            ANDROID to listOf(
                "Mozilla/5.0 (Linux; Android 13; SM-S901B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36",
                "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36",
                "Mozilla/5.0 (Linux; Android 13; SM-X700) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
            )
        )
        val PAGES = listOf(
            "/", "/home", "/products", "/product/details", "/cart",
            "/checkout", "/login", "/register", "/profile", "/settings",
            "/about", "/contact", "/blog", "/blog/article", "/search",
            "/category/electronics", "/category/clothing", "/category/books"
        )

        val ELEMENTS = listOf(
            "#buy-button", "#add-to-cart", "#checkout", "#login-button",
            "#signup-button", "#search-button", "#filter-button",
            "#sort-select", "#wishlist-icon", "#share-button",
            ".product-card", ".category-item", ".nav-link",
            "#newsletter-form", "#contact-form", "#video-player"
        )
        val REFERRERS = listOf(
            null,
            "https://www.google.com/search?q=example",
            "https://www.yandex.ru/search?text=example",
            "https://www.facebook.com/",
            "https://twitter.com/",
            "https://www.instagram.com/",
            "https://t.me/",
            "https://vk.com/",
            "https://www.youtube.com/",
            "https://www.linkedin.com/"
        )
        val BUTTON_TYPES = listOf("btn-primary", "btn-secondary", "btn-success", "btn-danger", "btn-warning")
        val BUTTON_SIZES = listOf("btn-sm", "btn-md", "btn-lg")
    }

}