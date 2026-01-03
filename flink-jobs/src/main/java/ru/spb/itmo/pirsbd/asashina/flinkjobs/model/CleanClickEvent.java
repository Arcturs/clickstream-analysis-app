package ru.spb.itmo.pirsbd.asashina.flinkjobs.model;

import java.time.LocalDateTime;

public class CleanClickEvent {

    private final String id;
    private final String type;
    private final LocalDateTime createdAt;
    private final String sessionId;
    private final Integer userId;
    private final String url;
    private final String deviceType;
    private final String elementId;
    private final Integer x;
    private final Integer y;
    private final String eventTitle;

    public CleanClickEvent(
            String id, String type, LocalDateTime createdAt, String sessionId, Integer userId, String url,
            String deviceType, String elementId, Integer x, Integer y, String eventTitle) {

        this.id = id;
        this.type = type;
        this.createdAt = createdAt;
        this.sessionId = sessionId;
        this.userId = userId;
        this.url = url;
        this.deviceType = deviceType;
        this.elementId = elementId;
        this.x = x;
        this.y = y;
        this.eventTitle = eventTitle;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public String getSessionId() {
        return sessionId;
    }

    public Integer getUserId() {
        return userId;
    }

    public String getUrl() {
        return url;
    }

    public String getDeviceType() {
        return deviceType;
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

    public String getEventTitle() {
        return eventTitle;
    }
}
