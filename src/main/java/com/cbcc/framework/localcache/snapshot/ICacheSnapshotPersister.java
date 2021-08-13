package com.cbcc.framework.localcache.snapshot;

import com.cbcc.framework.localcache.event.UpdateEvent;

import java.io.InputStream;

public interface ICacheSnapshotPersister {

    Snapshot createSnapshot(String cacheName, UpdateEvent event, InputStream input, String digest);

    Snapshot getLastestSnapshot(String cacheName);

    InputStream getInputStream(Snapshot snapshot);

}
