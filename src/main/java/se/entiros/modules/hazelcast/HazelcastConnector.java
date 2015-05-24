package se.entiros.modules.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import org.apache.log4j.Logger;
import org.mule.DefaultMuleEvent;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Source;
import org.mule.api.annotations.param.Default;
import org.mule.api.callback.SourceCallback;
import org.mule.api.callback.StopSourceCallback;
import org.mule.construct.Flow;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Hazelcast Anypoint Connector
 *
 * @author Petter Alstermark, Entiros AB
 */
@Connector(name = "hazelcast", friendlyName = "Hazelcast", schemaVersion = "current", minMuleVersion = "3.5.0")
public class HazelcastConnector {
    private static final Logger logger = Logger.getLogger(HazelcastConnector.class);

    /**
     * Timeout Unit
     */
    public enum TimeoutUnit {
        milliseconds(TimeUnit.MILLISECONDS),
        seconds(TimeUnit.SECONDS),
        minutes(TimeUnit.MINUTES);

        private TimeUnit unit;

        TimeoutUnit(TimeUnit unit) {
            this.unit = unit;
        }

        /**
         * @return TimeUnit
         */
        public TimeUnit getUnit() {
            return unit;
        }
    }

    private static HazelcastInstance instance;

    /**
     * @return New Hazelcast Instance
     */
    public static HazelcastInstance getHazelcastInstance() {
        if (instance == null) {
            Config config = new Config();
            config.setProperty("hazelcast.logging.type", "log4j");

            instance = Hazelcast.newHazelcastInstance(config);
        }
        return instance;
    }

    /**
     * Assign value to key in map
     *
     * @param map   map name
     * @param key   key
     * @param value value
     * @return clone of previous value
     */
    @Processor
    public Object mapPut(String map, Object key, Object value) {
        return getHazelcastInstance().getMap(map).put(key, value);
    }

    /**
     * Get value assigned to key in map
     *
     * @param map map name
     * @param key key
     * @return clone of value assigned to key
     */
    @SuppressWarnings("unchecked")
    @Processor
    public Object mapGet(String map, Object key) {
        return getHazelcastInstance().getMap(map).get(key);
    }

    /**
     * Insert value element into queue, waiting up to the specified wait time if necessary for space to become available
     *
     * @param queue       queue name
     * @param value       value to be added
     * @param timeout     how long to wait before giving up
     * @param timeoutUnit timeout unit
     * @return true if successful, false otherwise
     * @throws InterruptedException if interrupted while waiting
     */
    @Processor
    public boolean queueOffer(String queue, Object value, Long timeout, @Default("milliseconds") TimeoutUnit timeoutUnit) throws InterruptedException {
        return getHazelcastInstance().getQueue(queue).offer(value, timeout, timeoutUnit.getUnit());
    }

    /**
     * Insert value element into queue, waiting for space to become available if necessary
     *
     * @param queue queue name
     * @param value value to be added
     * @throws InterruptedException if interrupted while waiting
     */
    @Processor
    public void queuePut(String queue, Object value) throws InterruptedException {
        getHazelcastInstance().getQueue(queue).put(value);
    }

    /**
     * Retrieves and remove the head of this queue, waiting up to the specified wait time if necessary for an element
     * to become available
     *
     * @param queue       queue name
     * @param timeout     how long to wait before giving up
     * @param timeoutUnit timeout unit
     * @return head of this queue, or {@code null} if the specified waiting time elapses before an element is available
     * @throws InterruptedException if interrupted while waiting
     */
    @SuppressWarnings("unchecked")
    @Processor
    public Object queuePoll(String queue, Long timeout, @Default("milliseconds") TimeoutUnit timeoutUnit) throws InterruptedException {
        return getHazelcastInstance().getQueue(queue).poll(timeout, timeoutUnit.getUnit());
    }

    /**
     * Retrieves and removes the head of this queue, waiting for an element to become available if necessary
     *
     * @param queue queue name
     * @return head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    @SuppressWarnings("unchecked")
    @Processor
    public Object queueTake(String queue) throws InterruptedException {
        return getHazelcastInstance().getQueue(queue).take();
    }

    /**
     * Retrieves and remove the head of this queue and trigger inbound
     *
     * @param sourceCallback {@link org.mule.api.callback.SourceCallback}
     * @param queue          queue name
     * @return {@link org.mule.api.callback.StopSourceCallback}
     */
    @Source
    public StopSourceCallback queueInbound(final SourceCallback sourceCallback, final String queue) {
        final IQueue<Object> hazelcastQueue = getHazelcastInstance().getQueue(queue);
        final AtomicBoolean isRunning = new AtomicBoolean(true);
        final Thread pollingThread = new Thread() {
            @Override
            public void run() {
                while (isRunning.get()) {
                    try {
                        // Poll for value
                        Object value = hazelcastQueue.poll(1000, TimeUnit.MILLISECONDS);

                        // Process value
                        if (value != null) {
                            sourceCallback.process(value);
                        }
                    } catch (InterruptedException e) {
                        logger.error(String.format("Interrupted while polling queue '%s'", queue), e);
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }
            }
        };
        pollingThread.start();

        // Stop polling thread
        return new StopSourceCallback() {
            @Override
            public void stop() throws Exception {
                isRunning.set(false);
            }
        };
    }

    /**
     * Publish value to all subscribers of this topic
     *
     * @param topic topic name
     * @param value value
     */
    @Processor
    public void topicPublish(String topic, Object value) {
        getHazelcastInstance().getTopic(topic).publish(value);
    }

    /**
     * Trigger inbound when a new element is published to this topic
     *
     * @param sourceCallback {@link org.mule.api.callback.SourceCallback}
     * @param topic          topic name
     * @return {@link org.mule.api.callback.StopSourceCallback}
     */
    @Source
    public StopSourceCallback topicInbound(final SourceCallback sourceCallback, final String topic) {
        final ITopic<Object> hazelcastTopic = getHazelcastInstance().getTopic(topic);
        final String listenerId = hazelcastTopic.addMessageListener(new MessageListener<Object>() {
            @Override
            public void onMessage(Message<Object> message) {
                try {
                    sourceCallback.process(message.getMessageObject());
                } catch (Exception e) {
                    logger.error(e);
                }
            }
        });

        // Remove listener
        return new StopSourceCallback() {
            @Override
            public void stop() throws Exception {
                hazelcastTopic.removeMessageListener(listenerId);
            }
        };
    }

    /**
     * Run specified flow inside a lock
     *
     * @param event {@link org.mule.api.MuleEvent}
     * @param lock  lock name
     * @param flow  flow name
     * @return {@link org.mule.api.MuleMessage}
     * @throws MuleException
     */
    @Processor
    @Inject
    public MuleEvent lock(MuleEvent event, String lock, String flow) throws MuleException {
        ILock hazelcastLock = getHazelcastInstance().getLock(lock);

        // Lock
        hazelcastLock.lock();
        try {
            // Lookup flow
            Flow muleFlow = (Flow) event.getMuleContext().getRegistry().lookupFlowConstruct(flow);

            // Run flow
            if (muleFlow != null) {
                return muleFlow.process(event);
            }
            // Flow does not exist
            else {
                throw new RuntimeException(String.format("Flow '%s' does not exist", flow));
            }
        } finally {
            // Unlock
            hazelcastLock.unlock();
        }
    }
}