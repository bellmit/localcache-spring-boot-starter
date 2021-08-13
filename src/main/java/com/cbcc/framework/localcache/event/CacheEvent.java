package com.cbcc.framework.localcache.event;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CacheEvent {

    private String managerId;
    private String cacheName;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "clazz")
    private Object payload;

    public CacheEvent() {}

    public CacheEvent(String cacheName, Object payload, String managerId) {
        this.cacheName = cacheName;
        this.payload = payload;
        this.managerId = managerId;
    }

}
