package com.cbcc.framework.localcache;

/**
 * 什么类型的数据适合做本地缓存？
 * （1）更新频率较低；
 * （2）数据总量较小；
 * （3）通常有访问整个集合的需求，例如：币别列表、国家列表、权限树；
 * （4）对访问性能要求极高；
 *
 * 警告：框架确保对同一个缓存对象的更新是串行的，但不能控制应用代码对缓存对象的并发读取，因此请使用线程安全的方式实现缓存对象。
 */
public interface ICacheSupport<C> {

    /**
     * 从数据源构建一个新的缓存对象。
     * checkpoint为true表示这次是为检查点创建的缓存对象，应用代码可以利用该信息做一些特殊处理，例如锁定数据源以便于创建一个完整的缓存对象。
     */
    C initCache(boolean checkpoint);

    /**
     * Cache对象的摘要值，不允许返回null。
     * 警告：数据相同的缓存对象必须返回相同的摘要值。
     */
    String digestCache(C cache);

    /**
     * 更新缓存对象，返回用于回滚的数据对象
     */
    default Object updateCache(C cache, Object update) {
        return null;
    }

    /**
     * 回滚缓存对象的更新操作
     */
    default void rollbackCache(C cache, Object undo) {

    }

}
