package com.cbcc.framework.localcache;

import com.cbcc.framework.encrypt.DefaultEncryptor;
import com.cbcc.framework.encrypt.IEncryptor;
import com.cbcc.framework.encrypt.NoopEncryptor;
import com.cbcc.framework.localcache.event.bus.ICacheEventBus;
import com.cbcc.framework.localcache.event.bus.RabbitCacheEventBus;
import com.cbcc.framework.localcache.event.store.ICacheEventStore;
import com.cbcc.framework.localcache.event.store.MySQLCacheEventStore;
import com.cbcc.framework.localcache.snapshot.FileStoreCacheSnapshotPersister;
import com.cbcc.framework.localcache.snapshot.ICacheSnapshotPersister;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

public class CacheAutoConfiguration {

    @Bean("cacheEventEncryptor")
    @ConditionalOnMissingBean(name = "cacheEventEncryptor")
    @ConditionalOnProperty(name = "localcache.event.encryptor.enabled", havingValue = "true", matchIfMissing = true)
    public IEncryptor cacheEventEncryptor(@Value("${localcache.event.encryptor.key}") String cacheEventEncryptKey,
                                          @Value("${localcache.event.encryptor.iv}") String cacheEventEncryptIV) {

        return new DefaultEncryptor(cacheEventEncryptKey, cacheEventEncryptIV);
    }

    @Bean("cacheEventEncryptor")
    @ConditionalOnMissingBean(name = "cacheEventEncryptor")
    @ConditionalOnProperty(name = "localcache.event.encryptor.enabled", havingValue = "false")
    public IEncryptor cacheEventNoopEncryptor() {
        return new NoopEncryptor();
    }

    @Bean
    @ConditionalOnProperty(name = "localcache.event.store.type", havingValue = "mysql", matchIfMissing = true)
    public ICacheEventStore mysqlCacheEventStore() {
        return new MySQLCacheEventStore();
    }

    @Bean
    @ConditionalOnProperty(name = "localcache.event.bus.type", havingValue = "rabbit", matchIfMissing = true)
    public ICacheEventBus rabbitCacheEventBus() {
        return new RabbitCacheEventBus();
    }

    @Bean
    @ConditionalOnProperty(name = "localcache.snapshot.persister", havingValue = "filestore", matchIfMissing = true)
    public ICacheSnapshotPersister fileStoreCacheSnapshotPersister() {
        return new FileStoreCacheSnapshotPersister();
    }

    @Bean
    public CacheManagerConfiguration defaultCacheManagerConfiguration(ICacheEventStore eventStore,
                                                                      ICacheEventBus eventBus, 
                                                                      ICacheSnapshotPersister snapshotPersister,
                                                                      @Value("${localcache.devMode}") boolean devMode) {

        CacheManagerConfiguration defaultConfig = new CacheManagerConfiguration();
        defaultConfig.setEventStore(eventStore);
        defaultConfig.setEventBus(eventBus);
        defaultConfig.setSnapshotPersister(snapshotPersister);
        defaultConfig.setDevMode(devMode);
        return defaultConfig;
    }

}
