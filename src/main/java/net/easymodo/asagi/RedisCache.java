package net.easymodo.asagi;

import net.easymodo.asagi.exception.BoardInitException;
import net.easymodo.asagi.model.DeletedPost;
import net.easymodo.asagi.model.Media;
import net.easymodo.asagi.model.MediaPost;
import net.easymodo.asagi.model.Topic;
import net.easymodo.asagi.settings.BoardSettings;
import net.easymodo.asagi.settings.RedisCacheSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class RedisCache implements DB {
    private final RedisCacheSettings settings;

    private JedisPool redisPool;

    public RedisCache(RedisCacheSettings conectionSettings, BoardSettings boardSettings) {
        final Logger LOGGER = LoggerFactory.getLogger(RedisCache.class);

        this.settings = conectionSettings;

        try {
            init(null, null, boardSettings);
        } catch (BoardInitException e) {
            LOGGER.error("Could not connect to redis", e);
        }
    }

    public void wrap(SQL sqlDb) {

    }

    @Override
    public void connect() {
        redisPool.getResource().connect();
    }

    @Override
    public void reconnect() {
        redisPool.getResource().connect();
    }

    @Override
    public void init(String connStr, String path, BoardSettings info) throws BoardInitException {
        redisPool = new JedisPool(new JedisPoolConfig(), settings.host, settings.port, Protocol.DEFAULT_TIMEOUT, settings.password);
    }

    @Override
    public void insert(Topic topic) {
        Jedis redis = redisPool.getResource();
        if (redis.isConnected()) {
            // put data into redis
        }
    }

    @Override
    public void markDeleted(DeletedPost post) {

    }

    @Override
    public Media getMedia(MediaPost post) {
        return null;
    }
}
