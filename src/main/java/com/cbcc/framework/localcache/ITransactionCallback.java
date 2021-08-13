package com.cbcc.framework.localcache;

public interface ITransactionCallback {

    default void commit() {

    }

    default void rollback() {

    }

}
