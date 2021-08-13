package com.cbcc.framework.localcache;

import org.springframework.core.Ordered;
import org.springframework.transaction.reactive.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionContext {

    /**
     * TransactionSynchronizationManager.registerSynchronization注册的钩子是无序的（Set），需特殊处理
      */
    private final List<ITransactionCallback> callbacks = new ArrayList<>();
    private final Map<String, Object> cacheMap = new HashMap<>();

    public void addCallback(ITransactionCallback callback) {
        callbacks.add(callback);
    }

    public Object getCache(String cacheName) {
        return cacheMap.get(cacheName);
    }

    public void setCache(String cacheName, Object cache) {
        cacheMap.put(cacheName, cache);
    }

    private static final ThreadLocal<TransactionContext> CONTEXT = new ThreadLocal<TransactionContext>() {
        @Override
        protected TransactionContext initialValue() {
            TransactionContext ctx = new TransactionContext();

            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronizationAdapter() {

                        @Override
                        public int getOrder() {
                            return Ordered.HIGHEST_PRECEDENCE;
                        }

                        @Override
                        public void afterCompletion(int status) {
                            try {
                                TransactionContext ctx = CONTEXT.get();

                                if (TransactionSynchronization.STATUS_COMMITTED == status) {
                                    for (ITransactionCallback callback : ctx.callbacks) {
                                        callback.commit();
                                    }
                                } else {
                                    for (ITransactionCallback callback : ctx.callbacks) {
                                        callback.rollback();
                                    }
                                }
                            } finally {
                                CONTEXT.remove();
                            }
                        }

                    });

            return ctx;
        }
    };

    /**
     * 若在非事务上下文中调用则返回null
     */
    public static TransactionContext get() {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            return null;
        }

        return CONTEXT.get();
    }

}
