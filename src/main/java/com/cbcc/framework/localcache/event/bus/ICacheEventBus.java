package com.cbcc.framework.localcache.event.bus;

import com.cbcc.framework.localcache.event.CacheEvent;
import com.cbcc.framework.localcache.event.ICacheEventListener;

public interface ICacheEventBus {

    void addEventListener(String cacheName, ICacheEventListener listener);

    void publishEvent(CacheEvent event);

}
