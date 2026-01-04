package ru.spb.itmo.pirsbd.asashina.flinkjobs.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.time.LocalDateTime;
import java.util.Map;

@Table(keyspace = "clickstream", name = "click_events")
public class CassandraClickEvent {

    @Column(name = "user_id")
    private Integer userId;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "id")
    private String id;

    @Column(name = "type")
    private String type;

    @Column(name = "received_at")
    private LocalDateTime receivedAt;

    @Column(name = "session_id")
    private String sessionId;

    @Column(name = "ip")
    private String ip;

    @Column(name = "url")
    private String url;

    @Column(name = "referrer")
    private String referrer;

    @Column(name = "device_type")
    private String deviceType;

    @Column(name = "user_agent")
    private String userAgent;

    @Column(name = "event_title")
    private String eventTitle;

    @Column(name = "element_id")
    private String elementId;

    @Column(name = "x")
    private Integer x;

    @Column(name = "y")
    private Integer y;

    @Column(name = "element_text")
    private String elementText;

    @Column(name = "element_class")
    private String elementClass;
    
    @Column(name = "page_title")
    private String pageTitle;

    @Column(name = "viewport_width")
    private Integer viewportWidth;

    @Column(name = "viewport_height")
    private Integer viewportHeight;

    @Column(name = "scroll_position")
    private Double scrollPosition;

    @Column(name = "timestamp_offset")
    private Long timestampOffset;

    @Column(name = "metadat")
    private Map<String, String> metadata;

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

    public String getElementClass() {
        return elementClass;
    }

    public String getElementText() {
        return elementText;
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

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setReceivedAt(LocalDateTime receivedAt) {
        this.receivedAt = receivedAt;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public void setEventTitle(String eventTitle) {
        this.eventTitle = eventTitle;
    }

    public void setElementId(String elementId) {
        this.elementId = elementId;
    }

    public void setX(Integer x) {
        this.x = x;
    }

    public void setY(Integer y) {
        this.y = y;
    }

    public void setElementText(String elementText) {
        this.elementText = elementText;
    }

    public void setElementClass(String elementClass) {
        this.elementClass = elementClass;
    }

    public void setPageTitle(String pageTitle) {
        this.pageTitle = pageTitle;
    }

    public void setViewportWidth(Integer viewportWidth) {
        this.viewportWidth = viewportWidth;
    }

    public void setViewportHeight(Integer viewportHeight) {
        this.viewportHeight = viewportHeight;
    }

    public void setScrollPosition(Double scrollPosition) {
        this.scrollPosition = scrollPosition;
    }

    public void setTimestampOffset(Long timestampOffset) {
        this.timestampOffset = timestampOffset;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }
}
