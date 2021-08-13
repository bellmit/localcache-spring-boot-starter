package com.cbcc.framework.localcache.snapshot;

import com.cbcc.framework.filestore.DataProcessor;
import com.cbcc.framework.filestore.IFileStore;
import com.cbcc.framework.filestore.Metadata;
import com.cbcc.framework.utils.JsonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.InputStream;
import java.util.*;

public class FileStoreCacheSnapshotPersister extends RollingCacheSnapshotPersister {

    private final static String VERSION_FILE = "version";

    @Autowired
    private IFileStore fileStore;

    @Value("${localcache.snapshot.filestore.bucket:local-cache}")
    private String bucket;

    @Value("${localcache.snapshot.filestore.dataProcessor:COMPRESSED_AFTER_ENCRYPTED}")
    private DataProcessor dataProcessor;

    private String getVersionFilePath(String cacheName) {
        return "/" + cacheName + "/" + VERSION_FILE;
    }

    private String getDataFilePath(String cacheName) {
        return "/" + cacheName + "/data/";
    }

    private String getPath(Snapshot snapshot) {
        return "/" + snapshot.getCacheName() + "/data/" + snapshot.getId();
    }

    @Override
    public Snapshot getLastestSnapshot(String cacheName) {
        String content = fileStore.readContent(bucket, getVersionFilePath(cacheName), "UTF-8", dataProcessor);
        if (content == null) {
            return null;
        }

        return JsonUtil.toBean(content, Snapshot.class);
    }

    @Override
    public InputStream getInputStream(Snapshot snapshot) {
        return fileStore.getInputStream(bucket, getPath(snapshot), dataProcessor);
    }

    @Override
    protected void doCreateSnapshot(Snapshot snapshot, InputStream input) {
        Metadata metadata = convertToMetadata(snapshot);
        fileStore.save(bucket, getPath(snapshot), input, metadata, dataProcessor);

        String content = JsonUtil.toJson(snapshot);
        fileStore.saveContent(bucket, getVersionFilePath(snapshot.getCacheName()),
                content, "UTF-8", null, dataProcessor);
    }

    @Override
    protected List<Snapshot> getSnapshotList(String cacheName) {
        List<Metadata> list = fileStore.listMetadata(bucket, getDataFilePath(cacheName));
        List<Snapshot> result = new ArrayList<>();
        for (Metadata metadata : list) {
            result.add(convertToSnapshot(metadata));
        }

        return result;
    }

    @Override
    protected void deleteSnapshot(String cacheName, Snapshot snapshot) {
        fileStore.delete(bucket, getPath(snapshot));
    }

    private Metadata convertToMetadata(Snapshot snapshot) {
        Map<String, String> m = new HashMap<String, String>();
        m.put("id", snapshot.getId());
        m.put("cacheName", snapshot.getCacheName());
        m.put("time", Long.toString(snapshot.getTime().getTime()));
        m.put("digest", snapshot.getDigest());

        if (snapshot.getEventId() != null) {
            m.put("eventId", snapshot.getEventId().toString());
        }

        Metadata metadata = new Metadata();
        metadata.setUserMetadata(m);
        return metadata;
    }

    private Snapshot convertToSnapshot(Metadata metadata) {
        Map<String, String> m = metadata.getUserMetadata();

        Snapshot snapshot = new Snapshot();
        snapshot.setId(m.get("id"));
        snapshot.setCacheName(m.get("cacheName"));
        snapshot.setTime(new Date(Long.parseLong(m.get("time"))));
        snapshot.setDigest(m.get("digest"));

        String eventId = m.get("eventId");
        if (eventId != null) {
            snapshot.setEventId(Long.valueOf(eventId));
        }

        return snapshot;
    }

}
