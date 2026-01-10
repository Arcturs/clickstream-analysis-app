package ru.spb.itmo.pirsbd.asashina.api.model.entity

import org.springframework.data.cassandra.core.cql.Ordering
import org.springframework.data.cassandra.core.cql.PrimaryKeyType
import org.springframework.data.cassandra.core.mapping.CassandraType
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn
import org.springframework.data.cassandra.core.mapping.Table
import java.time.LocalDateTime

@Table("click_events")
data class ClickEvent(

    @PrimaryKey
    var key: ClickEventKey? = null,

    var type: String? = null,

    @Column("received_at")
    var receivedAt: LocalDateTime? = null,

    @Column("session_id")
    var sessionId: String? = null,

    var ip: String? = null,

    var url: String? = null,

    var referrer: String? = null,

    @Column("device_type")
    var deviceType: String? = null,

    @Column("user_agent")
    var userAgent: String? = null,

    @Column("event_title")
    var eventTitle: String? = null,

    @Column("element_id")
    var elementId: String? = null,

    var x: Int? = null,

    var y: Int? = null,

    @Column("element_text")
    var elementText: String? = null,

    @Column("element_class")
    var elementClass: String? = null,

    @Column("page_title")
    var pageTitle: String? = null,

    @Column("viewport_width")
    var viewportWidth: Int? = null,

    @Column("viewport_height")
    var viewportHeight: Int? = null,

    @Column("scroll_position")
    var scrollPosition: Double? = null,

    @Column("timestamp_offset")
    var timestampOffset: Long? = null,

    @CassandraType(type = CassandraType.Name.MAP, typeArguments = [CassandraType.Name.TEXT, CassandraType.Name.TEXT])
    var metadata: Map<String, String>? = null
)

@PrimaryKeyClass
data class ClickEventKey(
    @PrimaryKeyColumn(name = "user_id", type = PrimaryKeyType.PARTITIONED, ordinal = 0)
    var userId: Int? = null,

    @PrimaryKeyColumn(
        name = "created_at",
        type = PrimaryKeyType.CLUSTERED,
        ordering = Ordering.DESCENDING,
        ordinal = 1
    )
    var createdAt: LocalDateTime? = null,

    @PrimaryKeyColumn(
        name = "id",
        type = PrimaryKeyType.CLUSTERED,
        ordering = Ordering.ASCENDING,
        ordinal = 2
    )
    var id: String? = null
)