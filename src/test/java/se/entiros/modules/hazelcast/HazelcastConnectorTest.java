package se.entiros.modules.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import org.mule.modules.tests.ConnectorTestCase;

import org.junit.Test;
import org.mule.transport.NullPayload;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HazelcastConnectorTest extends ConnectorTestCase {
    private HazelcastInstance instance = HazelcastConnector.getHazelcastInstance();

    @Override
    protected String getConfigResources() {
        return "hazelcast-config.xml";
    }

    @Test
    public void testMap() throws Exception {
        assertEquals(NullPayload.getInstance(), runFlow("map-put", "myValue").getMessage().getPayload());
        assertEquals("myValue", runFlow("map-get").getMessage().getPayload());
    }

    @Test
    public void testQueue() throws Exception {
        assertTrue((Boolean) runFlow("queue-offer", "MyMessage").getMessage().getPayload());
        assertEquals("MyMessage", runFlow("queue-poll").getMessage().getPayload());

        runFlow("queue-put", "MyMessage2");
        assertEquals("MyMessage2", runFlow("queue-take").getMessage().getPayload());

        runFlow("queue-offer-inbound", "MyMessage3");
        assertEquals("MyMessage3", muleContext.getClient().request("vm://inboundQueue", 5000).getPayload());
    }

    @Test
    public void testTopic() throws Exception {
        runFlow("topic-publish", "myTopicValue");
        assertEquals("myTopicValue", muleContext.getClient().request("vm://inboundTopic", 5000).getPayload());
    }

    @Test
    public void testLock() throws Exception {
        assertEquals("Locked", runFlow("lock").getMessage().getPayload());
        assertEquals("Locked", muleContext.getClient().request("vm://lockVM", 5000).getPayload());
    }
}
