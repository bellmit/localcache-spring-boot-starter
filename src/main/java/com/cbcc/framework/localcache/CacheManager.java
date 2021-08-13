package com.cbcc.framework.localcache;

import com.cbcc.framework.localcache.event.*;
import com.cbcc.framework.localcache.event.bus.ICacheEventBus;
import com.cbcc.framework.localcache.event.store.ICacheEventStore;
import com.cbcc.framework.localcache.snapshot.ICacheSnapshotPersister;
import com.cbcc.framework.localcache.snapshot.Snapshot;
import com.cbcc.framework.utils.GUID;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Description;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class CacheManager<C> implements ICacheEventListener {

    private static final Logger logger = LoggerFactory.getLogger(CacheManager.class);

    private static final String PATTERN_CACHE_NAME = "[a-zA-Z]+[a-zA-Z_0-9\\-]";
    private static final Object CACHE_UNREADY = new Object();

    public static class CacheManagerBuilder<C> {

        private final String cacheName;
        private final ICacheSupport<C> support;
        private ICacheEventStore eventStore;
        private ICacheEventBus eventBus;
        private ICacheSnapshotPersister snapshotPersister;
        private boolean devMode;

        private CacheManagerBuilder(String cacheName, ICacheSupport<C> support, CacheManagerConfiguration defaultConfig) {
            if (!Pattern.compile(PATTERN_CACHE_NAME).matcher(cacheName).matches()) {
                throw new IllegalArgumentException("Illegal cache name: " + cacheName);
            }

            if (cacheName.length() > 40) {
                throw new IllegalArgumentException("The cache name could not exceed 40 characters: " + cacheName);
            }

            this.cacheName = cacheName;
            this.support = support;

            if (defaultConfig != null) {
                if (defaultConfig.getEventStore() != null) {
                    this.eventStore = defaultConfig.getEventStore();
                }

                if (defaultConfig.getEventBus() != null) {
                    this.eventBus = defaultConfig.getEventBus();
                }

                if (support instanceof ISnapshotableCacheSupport
                        && defaultConfig.getSnapshotPersister() != null) {

                    this.snapshotPersister = defaultConfig.getSnapshotPersister();
                }

                this.devMode = defaultConfig.isDevMode();
            }
        }

        public CacheManagerBuilder<C> eventStore(ICacheEventStore eventStore) {
            this.eventStore = eventStore;
            return this;
        }

        public CacheManagerBuilder<C> eventBus(ICacheEventBus eventBus) {
            this.eventBus = eventBus;
            return this;
        }

        public CacheManagerBuilder<C> snapshotPersister(ICacheSnapshotPersister snapshotPersister) {
            if (!(support instanceof ISnapshotableCacheSupport)) {
                throw new IllegalArgumentException("ISnapshotableCacheSupport required");
            }

            this.snapshotPersister = snapshotPersister;
            return this;
        }

        public CacheManagerBuilder<C> devMode(boolean devMode) {
            this.devMode = devMode;
            return this;
        }

        public CacheManager<C> build() {
            if (eventStore == null) {
                throw new IllegalStateException("eventStore required");
            }

            if (eventBus == null) {
                throw new IllegalStateException("eventBus required");
            }

            if (support instanceof ISnapshotableCacheSupport) {
                if (snapshotPersister == null) {
                    throw new IllegalStateException("snapshotPersister required");
                }
            }

            CacheManager<C> cm = new CacheManager<>(cacheName,
                    support, eventStore, eventBus, snapshotPersister, devMode);

            eventBus.addEventListener(cacheName, cm);
            return cm;
        }

    }

    public static <C> CacheManagerBuilder<C> newBuilder(String cacheName,
                                                        ICacheSupport<C> support,
                                                        CacheManagerConfiguration defaultConfig) {

        return new CacheManagerBuilder<C>(cacheName, support, defaultConfig);
    }

    public static <C> CacheManagerBuilder<C> newBuilder(String cacheName,
                                                        ICacheSupport<C> support) {

        return newBuilder(cacheName, support, null);
    }

    @Getter
    private static class CacheInfo<C> {
        private final C cache;
        private Long eventId;
        private String digest;

        public CacheInfo(C cache, Long eventId, String digest) {
            this.cache = cache;
            this.eventId = eventId;
            this.digest = digest;
        }

        public CacheInfo(C cache, Long eventId) {
            this(cache, eventId, null);
        }

        public void setEventId(Long eventId) {
            this.eventId = eventId;
        }

        public void setDigest(String digest) {
            this.digest = digest;
        }
    }

    private final String id = GUID.get();
    private final String cacheName;
    private final ICacheSupport<C> support;
    private final ICacheEventStore eventStore;
    private final ICacheEventBus eventBus;
    private final ICacheSnapshotPersister snapshotPersister;
    private final boolean devMode;

    // volatile
    private volatile CacheInfo cacheInfo;

    private CacheManager(String cacheName,
                         ICacheSupport<C> support,
                         ICacheEventStore eventStore,
                         ICacheEventBus eventBus,
                         ICacheSnapshotPersister snapshotPersister,
                         boolean devMode) {

        this.cacheName = cacheName;
        this.support = support;
        this.eventStore = eventStore;
        this.eventBus = eventBus;
        this.snapshotPersister = snapshotPersister;
        this.devMode = devMode;
    }

    @Override
    public void handle(CacheEvent e) {
        CacheInfo<C> ci = cacheInfo;
        if (ci == null) {
            return;
        }

        Object payload = e.getPayload();
        if (payload instanceof Checkpoint) {
            Checkpoint checkpoint = (Checkpoint) payload;

            if (Objects.equals(checkpoint.getEventId(), ci.getEventId())
                    || checkpoint.getEventId() == null) {

                synchronized (ci) {
                    String digest = ci.getDigest();
                    if (digest == null) {
                        digest = support.digestCache(ci.getCache());
                        ci.setDigest(digest);
                    }

                    if (!Objects.equals(digest, checkpoint.getDigest())) {
                        logger.warn("Unmatched digest, cache: " + e.getCacheName());
                        cacheInfo = null;
                    } else if (!Objects.equals(ci.getEventId(), checkpoint.getEventId())){
                        ci.setEventId(checkpoint.getEventId());
                    }

                    return;
                }
            }

            Long afterId = ci.getEventId();
            if (afterId != null && afterId.compareTo(checkpoint.getEventId()) > 0) {
                return;
            }

            if (eventStore.detectsFlushAfter(cacheName, afterId)) {
                cacheInfo = null;
                return;
            }

            boolean stopped = false;
            List<UpdateEvent> eventList;
            do {
                eventList = eventStore.getUpdateEventList(cacheName, afterId, 100);
                for (UpdateEvent event : eventList) {
                    if (checkpoint.getEventId() < event.getId()) {
                        stopped = true;
                        break;
                    }

                    synchronized (ci) {
                        support.updateCache(ci.getCache(), event.getData());
                        ci.setEventId(event.getId());
                        ci.setDigest(null);
                    }

                    afterId = event.getId();
                }
            } while (!stopped && eventList.size() > 0);
        } else {
            if (id.equals(e.getManagerId())) {
                return;
            }

            UpdateEvent event = (UpdateEvent) payload;

            if (UpdateMode.FLUSH.equals(event.getUpdateMode())) {
                cacheInfo = null;
                return;
            }

            synchronized (ci) {
                support.updateCache(ci.getCache(), event.getData());
                ci.setEventId(event.getId());
                ci.setDigest(null);
            }
        }
    }

    public C getCache() {
        TransactionContext tc = TransactionContext.get();
        if (tc != null) {
            Object c = tc.getCache(cacheName);
            if (c != null) {
                if (CACHE_UNREADY == c) {
                    c = support.initCache(false);
                    tc.setCache(cacheName, c);
                }

                return (C) c;
            }
        }

        CacheInfo<C> ci = cacheInfo;
        if (ci == null) {
            synchronized (this) {
                ci = cacheInfo;
                if (ci == null) {
                    ci = buildCache();
                    cacheInfo = ci;
                }
            }
        }

        return ci.getCache();
    }

    private CacheInfo buildCache() {
        UpdateEvent lastEvent = eventStore.getLastUpdateEvent(cacheName);
        Long lastEventId = lastEvent == null ? null : lastEvent.getId();

        // 如果支持快照，则从最新的快照+后续事件快速恢复
        if (snapshotPersister != null) {
            Snapshot snapshot = snapshotPersister.getLastestSnapshot(cacheName);
            if (snapshot != null) {
                if (!eventStore.detectsFlushAfter(cacheName, snapshot.getEventId())) {
                    InputStream input = snapshotPersister.getInputStream(snapshot);
                    C cache;
                    try {
                        cache = ((ISnapshotableCacheSupport<C>) support).deserializeCache(input);
                    } finally {
                        if (input != null) {
                            try {
                                input.close();
                            } catch (IOException _) {
                            }
                        }
                    }

                    if (Objects.equals(lastEventId, snapshot.getEventId())) {
                        return new CacheInfo(cache, snapshot.getEventId(), snapshot.getDigest());
                    } else if (lastEventId == null) {
                        return new CacheInfo(cache, snapshot.getEventId());
                    }

                    Long afterId = snapshot.getEventId();
                    List<UpdateEvent> eventList;
                    do {
                        eventList = eventStore.getUpdateEventList(cacheName, afterId, 100);
                        for (UpdateEvent event : eventList) {
                            if (lastEventId < event.getId()) {
                                return new CacheInfo(cache, afterId);
                            }

                            support.updateCache(cache, event.getData());
                            afterId = event.getId();
                        }
                    } while (eventList.size() > 0);

                    return new CacheInfo(cache, afterId);
                }
            }
        }

        C cache = support.initCache(false);
        return new CacheInfo(cache, lastEventId);
    }

    public void updateCache(Object update) {
        if (update == null) {
            throw new IllegalArgumentException("The update object required");
        }

        UpdateEvent event = eventStore.createUpdateEvent(cacheName, UpdateMode.UPDATE, update);

        TransactionContext tc = TransactionContext.get();
        if (tc != null) {
            C tmpCache = null;
            Object c = tc.getCache(cacheName);
            if (c != null && CACHE_UNREADY != c) {
                tmpCache = (C) c;
            }

            Object tmpUndo = null;
            if (tmpCache != null) {
                tmpUndo = support.updateCache(tmpCache, update);
            }

            Long prevEventId = null;
            CacheInfo<C> ci = cacheInfo;
            Object undo = null;
            if (ci != null) {
                synchronized (ci) {
                    prevEventId = ci.getEventId();
                    undo = support.updateCache(ci.getCache(), update);
                    ci.setEventId(event.getId());
                    ci.setDigest(null);
                }
            }

            final C tmpCache2 = tmpCache;
            final Object tmpUndo2 = tmpUndo;
            final Object undo2 = undo;
            final Long prevEventId2 = prevEventId;
            tc.addCallback(new ITransactionCallback() {
                @Override
                public void commit() {
                    eventBus.publishEvent(new CacheEvent(cacheName, event, id));
                }

                @Override
                public void rollback() {
                    if (tmpCache2 != null) {
                        support.rollbackCache(tmpCache2, tmpUndo2);
                    }

                    if (ci != null) {
                        synchronized (ci) {
                            support.rollbackCache(ci.getCache(), undo2);
                            ci.setEventId(prevEventId2);
                        }
                    }
                }
            });
        } else {
            CacheInfo<C> ci = cacheInfo;
            if (ci != null) {
                synchronized (ci) {
                    support.updateCache(ci.getCache(), update);
                }
            }

            eventBus.publishEvent(new CacheEvent(cacheName, event, id));
        }
    }

    /**
     * 要谨慎处理事务上下文中调用flushCache导致的缓存脏数据问题。考虑如下时序：
     * （1）线程A开启事务，更新数据库，然后调用CacheManager.flushCache()；
     * （2）其它线程调用CacheManager.getCache()，导致缓存的初始化，但由于线程A的事务未提交，缓存数据是旧的；
     * （3）线程A提交事务，但缓存已经初始化了，一直保留旧数据；
     *
     * 警告：调用该方法将清除所有节点缓存，导致大量缓存初始化操作。
     */
    @Description("This method may cause massive cache initializations")
    public void flushCache() {
        UpdateEvent event = eventStore.createUpdateEvent(cacheName, UpdateMode.FLUSH, null);

        TransactionContext tc = TransactionContext.get();
        if (tc == null) {
            publishFlushEvent(event);
            return;
        }

        Object c = tc.getCache(cacheName);
        if (CACHE_UNREADY != c) {
            tc.setCache(cacheName, CACHE_UNREADY);
        }

        tc.addCallback(new ITransactionCallback() {
            @Override
            public void commit() {
                publishFlushEvent(event);
            }
        });
    }

    private void publishFlushEvent(UpdateEvent event) {
        // 如果支持快照，则打个快照再广播事件
        if (snapshotPersister != null) {
            C cache = support.initCache(true);
            String digest = support.digestCache(cache);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ((ISnapshotableCacheSupport<C>) support).serializeCache(cache, output);
            snapshotPersister.createSnapshot(cacheName, event,
                    new ByteArrayInputStream(output.toByteArray()), digest);
        }

        cacheInfo = null;
        eventBus.publishEvent(new CacheEvent(cacheName, event, id));
    }

    public void createCheckpoint() {
        UpdateEvent lastEvent = eventStore.getLastUpdateEvent(cacheName);
        Long lastEventId = lastEvent == null ? null : lastEvent.getId();

        C cache = support.initCache(true);
        String digest = support.digestCache(cache);

        if (devMode) {
            logger.info("<<<<<< This is for dev mode");

            C cache2 = support.initCache(true);
            UpdateEvent lastEvent2 = eventStore.getLastUpdateEvent(cacheName);
            Long lastEventId2 = lastEvent2 == null ? null : lastEvent2.getId();
            if (Objects.equals(lastEventId, lastEventId2)) {
                String digest2 = support.digestCache(cache2);
                if (!Objects.equals(digest, digest2)) {
                    logger.error("Different digests for the same cache data built by initCache(true/true): "
                            + support.getClass());
                }
            }

            C cache3 = support.initCache(false);
            UpdateEvent lastEvent3 = eventStore.getLastUpdateEvent(cacheName);
            Long lastEventId3 = lastEvent3 == null ? null : lastEvent3.getId();
            if (Objects.equals(lastEventId, lastEventId3)) {
                String digest3 = support.digestCache(cache3);
                if (!Objects.equals(digest, digest3)) {
                    logger.error("Different digests for the same cache data built by initCache(true/false): "
                            + support.getClass());
                }
            }

            logger.info(">>>>>> This is for dev mode");
        }

        if (snapshotPersister != null) {
            Snapshot snapshot = snapshotPersister.getLastestSnapshot(cacheName);
            if (snapshot == null
                    || !Objects.equals(lastEventId, snapshot.getEventId())
                    || !Objects.equals(digest, snapshot.getDigest())) {

                ByteArrayOutputStream output = new ByteArrayOutputStream();
                ((ISnapshotableCacheSupport<C>) support).serializeCache(cache, output);
                byte[] bytes = output.toByteArray();
                snapshotPersister.createSnapshot(cacheName, lastEvent, new ByteArrayInputStream(bytes), digest);

                if (devMode) {
                    logger.info("<<<<<< This is for dev mode");

                    C cache2 = ((ISnapshotableCacheSupport<C>) support).deserializeCache(
                             new ByteArrayInputStream(bytes));
                    String digest2 = support.digestCache(cache2);
                    if (!Objects.equals(digest, digest2)) {
                        logger.error("The serialization and deserialization are unmatched: " + support.getClass());
                    }

                    C cache3 = ((ISnapshotableCacheSupport<C>) support).deserializeCache(
                            new ByteArrayInputStream(bytes));
                    String digest3 = support.digestCache(cache3);
                    if (!Objects.equals(digest2, digest3)) {
                        logger.error("Different digests for the same cache data built by deserializeCache(): "
                                + support.getClass());
                    }

                    logger.info(">>>>>> This is for dev mode");
                }
            }
        }

        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setCacheName(cacheName);
        checkpoint.setTime(new Date());
        checkpoint.setEventId(lastEventId);
        checkpoint.setDigest(digest);
        eventBus.publishEvent(new CacheEvent(cacheName, checkpoint, id));
    }

}
