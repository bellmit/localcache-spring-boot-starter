package com.cbcc.framework.localcache;

import com.cbcc.framework.localcache.event.bus.ICacheEventBus;
import com.cbcc.framework.localcache.event.store.ICacheEventStore;
import com.cbcc.framework.localcache.snapshot.ICacheSnapshotPersister;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CacheManagerConfiguration {

    private ICacheEventStore eventStore;
    private ICacheEventBus eventBus;
    private ICacheSnapshotPersister snapshotPersister;
    private boolean devMode;

}
