package ru.spb.itmo.pirsbd.asashina.api.model.entity

import org.springframework.data.cassandra.core.cql.Ordering
import org.springframework.data.cassandra.core.cql.PrimaryKeyType
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn
import org.springframework.data.cassandra.core.mapping.Table
import java.time.LocalDateTime

@Table("click_events_by_session")
data class ClickEventBySession(

    @PrimaryKey
    var key: ClickEventBySessionKey? = null,

    @Column("user_id")
    var userId: Int? = null,

    var type: String? = null,

    @Column("received_at")
    var receivedAt: LocalDateTime? = null,

    var ip: String? = null,

    var url: String? = null,

    var referrer: String? = null,

    @Column("device_type")
    var deviceType: String? = null,

    @Column("user_agent")
    var userAgent: String? = null,

    @Column("event_title")
    var eventTitle: String? = null
)

@PrimaryKeyClass
data class ClickEventBySessionKey(

    @PrimaryKeyColumn(name = "session_id", type = PrimaryKeyType.PARTITIONED, ordinal = 0)
    var sessionId: String? = null,

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