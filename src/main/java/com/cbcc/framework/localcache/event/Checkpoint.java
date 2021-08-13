package com.cbcc.framework.localcache.event;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
public class Checkpoint {

    private String cacheName;
    private Date time;
    private Long eventId;
    private String digest;

}
