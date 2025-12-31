package ru.spb.itmo.pirsbd.asashina.flinkjobs.model.entity;

import java.time.LocalDateTime;
import java.util.Map;

public class CassandraClickEvent {

    private final Integer userId;
    private final LocalDateTime createdAt;
    private final String id;
    private final String type;
    private final LocalDateTime receivedAt;
    private final String sessionId;
    private final String ip;
    private final String url;
    private final String referrer;
    private final String deviceType;
    private final String userAgent;
    private final String eventTitle;
    private final String elementId;
    private final Integer x;
    private final Integer y;
    private final String elementText;
    private final String elementClass;
    private final String pageTitle;
    private final Integer viewportWidth;
    private final Integer viewportHeight;
    private final Double scrollPosition;
    private final Long timestampOffset;
    private final Map<String, String> metadata;

    public CassandraClickEvent(
            Integer userId, LocalDateTime createdAt, String id, String type, LocalDateTime receivedAt, String sessionId,
            String ip, String url, String referrer, String deviceType, String userAgent, String eventTitle,
            String elementId, Integer x, Integer y, String elementText, String elementClass, String pageTitle,
            Integer viewportWidth, Integer viewportHeight, Double scrollPosition, Long timestampOffset,
            Map<String, String> metadata) {

        this.userId = userId;
        this.createdAt = createdAt;
        this.id = id;
        this.type = type;
        this.receivedAt = receivedAt;
        this.sessionId = sessionId;
        this.ip = ip;
        this.url = url;
        this.referrer = referrer;
        this.deviceType = deviceType;
        this.userAgent = userAgent;
        this.eventTitle = eventTitle;
        this.elementId = elementId;
        this.x = x;
        this.y = y;
        this.elementText = elementText;
        this.elementClass = elementClass;
        this.pageTitle = pageTitle;
        this.viewportWidth = viewportWidth;
        this.viewportHeight = viewportHeight;
        this.scrollPosition = scrollPosition;
        this.timestampOffset = timestampOffset;
        this.metadata = metadata;
    }
}
