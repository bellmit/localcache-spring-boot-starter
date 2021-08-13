package com.cbcc.framework.localcache.snapshot;

import com.cbcc.framework.localcache.event.UpdateEvent;
import com.cbcc.framework.utils.GUID;
import org.springframework.beans.factory.annotation.Value;

import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public abstract class RollingCacheSnapshotPersister implements ICacheSnapshotPersister {

    @Value("${localcache.snapshot.remains:3}")
    private int remains = 3;

    @Override
    public final Snapshot createSnapshot(String cacheName, UpdateEvent event, InputStream input, String digest) {
        Snapshot snapshot = new Snapshot();
        snapshot.setId(GUID.get());
        snapshot.setCacheName(cacheName);
        snapshot.setTime(new Date());
        snapshot.setDigest(digest);

        if (event != null) {
            snapshot.setEventId(event.getId());
        }

        doCreateSnapshot(snapshot, input);

        List<Snapshot> snapshotList = getSnapshotList(cacheName);
        Collections.sort(snapshotList, new Comparator<Snapshot>() {
            @Override
            public int compare(Snapshot o1, Snapshot o2) {
                return o1.getTime().compareTo(o2.getTime());
            }
        });

        int total = snapshotList.size();
        if (remains > 0 && total > remains) {
            for (int i = 0, n = total - remains; i < n; i++) {
                deleteSnapshot(cacheName, snapshotList.get(i));
            }
        }

        return snapshot;
    }

    protected abstract void doCreateSnapshot(Snapshot snapshot, InputStream input);
    protected abstract List<Snapshot> getSnapshotList(String cacheName);
    protected abstract void deleteSnapshot(String cacheName, Snapshot snapshot);

}
