package ru.spb.itmo.pirsbd.asashina.flinkjobs.model;

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

    public Integer getUserId() {
        return userId;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public LocalDateTime getReceivedAt() {
        return receivedAt;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getIp() {
        return ip;
    }

    public String getUrl() {
        return url;
    }

    public String getReferrer() {
        return referrer;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public String getEventTitle() {
        return eventTitle;
    }

    public String getElementId() {
        return elementId;
    }

    public Integer getX() {
        return x;
    }

    public Integer getY() {
        return y;
    }

    public String getElementText() {
        return elementText;
    }

    public String getElementClass() {
        return elementClass;
    }

    public String getPageTitle() {
        return pageTitle;
    }

    public Integer getViewportWidth() {
        return viewportWidth;
    }

    public Integer getViewportHeight() {
        return viewportHeight;
    }

    public Double getScrollPosition() {
        return scrollPosition;
    }

    public Long getTimestampOffset() {
        return timestampOffset;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }
}
