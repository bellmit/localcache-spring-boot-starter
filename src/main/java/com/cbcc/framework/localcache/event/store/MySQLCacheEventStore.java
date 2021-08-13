package com.cbcc.framework.localcache.event.store;

import com.cbcc.framework.encrypt.IEncryptor;
import com.cbcc.framework.localcache.event.UpdateEvent;
import com.cbcc.framework.localcache.event.UpdateMode;
import com.cbcc.framework.utils.JsonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MySQLCacheEventStore implements ICacheEventStore {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${localcache.event.store.mysql.tableName:CacheEvent}")
    private String tableName;

    @Autowired(required = false)
    @Qualifier("cacheEventEncryptor")
    private IEncryptor encryptor;

    @Override
    public UpdateEvent createUpdateEvent(String cacheName, UpdateMode updateMode, Object data) {
        UpdateEvent e = new UpdateEvent();
        e.setData(data);
        String str = JsonUtil.toJson(e);
        if (encryptor != null) {
            str = encryptor.encrypt(str);
        }

        final String str2 = str;
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(new PreparedStatementCreator() {
                @Override
                public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                    String sql = "insert into " + tableName + "(cacheName, updateMode, time, data) values(?, ?, now(3), ?)";
                    PreparedStatement ps = (PreparedStatement) connection.prepareStatement(sql,
                            Statement.RETURN_GENERATED_KEYS);

                    ps.setString(1, cacheName);
                    ps.setString(2, updateMode.name());
                    ps.setString(3, str2);
                    return ps;
                }
            }, keyHolder);

        Map<String, Object> m = jdbcTemplate.queryForMap(
                "select * from " + tableName + " where id=?", keyHolder.getKey());
        return convertToUpdateEvent(m);
    }

    private UpdateEvent convertToUpdateEvent(Map<String, Object> m) {
        if (m == null) {
            return null;
        }

        String s = (String) m.get("data");
        if (s == null) {
            return null;
        }

        if (encryptor != null) {
            s = encryptor.decrypt(s);
        }

        UpdateEvent event = JsonUtil.toBean(s, UpdateEvent.class);
        event.setId(Long.valueOf(m.get("id").toString()));
        event.setCacheName((String) m.get("cacheName"));
        event.setUpdateMode(UpdateMode.valueOf((String) m.get("updateMode")));
        event.setTime((Date) m.get("time"));
        return event;
    }

    @Override
    public UpdateEvent getLastUpdateEvent(String cacheName) {
        List<Map<String, Object>> list = jdbcTemplate.queryForList(
                "select * from " + tableName + " where cacheName=? order by id desc limit 1", cacheName);

        return list.size() > 0 ? convertToUpdateEvent(list.get(0)) : null;
    }

    @Override
    public List<UpdateEvent> getUpdateEventList(String cacheName, Long afterId, int limit) {
        List<Map<String, Object>> list = jdbcTemplate.queryForList(
                "select * from " + tableName + " where cacheName=? and id>? order by id limit ?",
                    cacheName, afterId == null ? 0L : afterId, limit);

        List<UpdateEvent> result = new ArrayList<>();
        for (Map<String, Object> m : list) {
            UpdateEvent e = convertToUpdateEvent(m);
            if (e != null) {
                result.add(e);
            }
        }

        return result;
    }

    @Override
    public boolean detectsFlushAfter(String cacheName, Long afterId) {
        List<Map<String, Object>> list = jdbcTemplate.queryForList(
                "select 1 from " + tableName + " where cacheName=? and updateMode=? and id>? limit 1",
                cacheName, UpdateMode.FLUSH.name(), afterId == null ? 0L : afterId);
        return list.size() > 0;
    }

}
