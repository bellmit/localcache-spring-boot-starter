package com.cbcc.framework.localcache;

import java.io.InputStream;
import java.io.OutputStream;

public interface ISnapshotableCacheSupport<C> extends ICacheSupport<C> {

    void serializeCache(C cache, OutputStream output);

    C deserializeCache(InputStream input);

}
