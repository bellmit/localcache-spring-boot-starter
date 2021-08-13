package com.cbcc.framework.localcache.event;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
public class UpdateEvent {

    private Long id;
    private String cacheName;
    private UpdateMode updateMode;
    private Date time;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "clazz")
    private Object data;

}
