drop table if exists CacheEvent;
create table CacheEvent (
    id bigint unsigned auto_increment,
    cacheName varchar(40) not null,
    updateMode varchar(10) not null,
    time datetime(3) not null default now(3),
    data text,
    primary key(id)
);

create index IX_CacheEvent_cacheName on CacheEvent(cacheName, id);
create index IX_CacheEvent_cacheName_updateMode on CacheEvent(cacheName, updateMode, id);