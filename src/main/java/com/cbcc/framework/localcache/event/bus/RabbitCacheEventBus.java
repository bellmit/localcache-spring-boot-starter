package com.cbcc.framework.localcache.event.bus;

import com.cbcc.framework.encrypt.IEncryptor;
import com.cbcc.framework.localcache.event.CacheEvent;
import com.cbcc.framework.localcache.event.ICacheEventListener;
import com.cbcc.framework.utils.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.ListenerContainerConsumerFailedEvent;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.*;

import java.util.*;

public class RabbitCacheEventBus implements ICacheEventBus,
        SmartLifecycle, ApplicationEventPublisherAware, ApplicationListener {

    private static final Logger logger = LoggerFactory.getLogger(RabbitCacheEventBus.class);

    private static class CacheMessageListenerContainer extends SimpleMessageListenerContainer {
    }

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private ConnectionFactory connectionFactory;

    @Value("${localcache.event.bus.rabbit.exchangeName:local_cache_event}")
    private String exchangeName;

    @Autowired(required = false)
    @Qualifier("cacheEventEncryptor")
    private IEncryptor encryptor;

    private final Map<String, List<ICacheEventListener>> listenersMap = new HashMap<>();

    private CacheMessageListenerContainer messageListenerContainer;
    private ApplicationEventPublisher applicationEventPublisher;

    private volatile boolean stopped = true;

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public void addEventListener(String cacheName, ICacheEventListener listener) {
        List<ICacheEventListener> listeners = listenersMap.get(cacheName);
        if (listeners == null) {
            listeners = new ArrayList<>();
            listenersMap.put(cacheName, listeners);
        }

        listeners.add(listener);
    }

    @Override
    public void publishEvent(CacheEvent event) {
        byte[] bytes;
        try {
            String str = JsonUtil.toJson(event);
            if (encryptor != null) {
                str = encryptor.encrypt(str);
            }

            bytes = str.getBytes("UTF-8");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        rabbitTemplate.convertAndSend(exchangeName, "", bytes);
    }

    @Override
    public void start() {
        initCacheEventListener();
        stopped = false;
    }

    private void initCacheEventListener() {
        Queue queue = rabbitAdmin.declareQueue();
        FanoutExchange exchange = new FanoutExchange(exchangeName);
        Binding binding = BindingBuilder.bind(queue).to(exchange);
        rabbitAdmin.declareExchange(exchange);
        rabbitAdmin.declareBinding(binding);

        messageListenerContainer = new CacheMessageListenerContainer();
        messageListenerContainer.setApplicationEventPublisher(applicationEventPublisher);
        messageListenerContainer.setConnectionFactory(connectionFactory);
        messageListenerContainer.setQueueNames(queue.getName());
        messageListenerContainer.setConcurrentConsumers(1);
        messageListenerContainer.setMaxConcurrentConsumers(1);

        messageListenerContainer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                CacheEvent event;
                try {
                    String str = new String(message.getBody(), "UTF-8");
                    if (encryptor != null) {
                        str = encryptor.decrypt(str);
                    }

                    event = JsonUtil.toBean(str, CacheEvent.class);
                } catch (Exception e) {
                    logger.error("Failed to read message", e);
                    return;
                }

                handleCacheEvent(event);
            }
        });

        messageListenerContainer.afterPropertiesSet();
        messageListenerContainer.start();
    }

    private void handleCacheEvent(CacheEvent event) {
        List<ICacheEventListener> listeners = listenersMap.get(event.getCacheName());
        if (listeners == null || listeners.size() == 0) {
            logger.warn("No listeners for cache event: " + event.getCacheName());
            return;
        }

        for (ICacheEventListener listener : listeners) {
            try {
                listener.handle(event);
            } catch (Exception e) {
                logger.error("Failed to handle cache event", e);
            }
        }
    }

    @Override
    public void stop() {
        messageListenerContainer.stop();
        stopped = true;
    }

    @Override
    public boolean isRunning() {
        return !stopped;
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof ListenerContainerConsumerFailedEvent) {
            ListenerContainerConsumerFailedEvent e = (ListenerContainerConsumerFailedEvent) event;
            if (e.isFatal() && (e.getSource() instanceof CacheMessageListenerContainer)) {
                CacheMessageListenerContainer listenerContainer = (CacheMessageListenerContainer) e.getSource();

                boolean retry;
                do {
                    try {
                        logger.warn("Restart the CacheMessageListenerContainer");
                        initCacheEventListener();
                        retry = false;
                    } catch (Exception ex) {
                        logger.error("Failed to restart the LocalCacheMessageListenerContainer", ex);
                        retry = true;

                        try {
                            Thread.sleep(1000L + new Random().nextInt(2000));
                        } catch (InterruptedException iex) {
                        }
                    }
                } while (retry);
            }
        }
    }

}
