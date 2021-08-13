package com.cbcc.framework.localcache.event.store;

import com.cbcc.framework.localcache.event.UpdateEvent;
import com.cbcc.framework.localcache.event.UpdateMode;

import java.util.List;

public interface ICacheEventStore {

    UpdateEvent createUpdateEvent(String cacheName, UpdateMode updateMode, Object data);

    UpdateEvent getLastUpdateEvent(String cacheName);

    List<UpdateEvent> getUpdateEventList(String cacheName, Long afterId, int limit);

    boolean detectsFlushAfter(String cacheName, Long afterId);

}
