package io.github.mattisonchao.pulsar.client.experiment;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Test
public class ClientStuckTest extends BrokerBase {

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() {
        super.cleanup();
    }

    @Test
    public void client_timeout_not_stuck_with_broker_2_8_3() throws PulsarClientException {
        // Disable this line when use specific local docker environment to test different client version.
        init("apachepulsar/pulsar:2.8.3");

        final String tp = "timeout_stuck_with_broker_2_8_3";
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerURL)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(tp)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(tp)
                .subscriptionName("sub-1")
                .subscribe();

        for (int i = 0; i < 100; i++) {
            producer.send((i + "").getBytes(StandardCharsets.UTF_8));
        }

        for (int i = 0; i < 100; i++) {
            final Message<byte[]> message = consumer.receive(1000, TimeUnit.MILLISECONDS);
            consumer.acknowledge(message);
            Assert.assertEquals(new String(message.getValue()), i + "");
        }
        // verify if consumer get stuck forever.
        final Message<byte[]> message1 = consumer.receive(1000, TimeUnit.MILLISECONDS);
        Assert.assertNull(message1);

        // verify if consumer get stuck forever.
        final Message<byte[]> message2 = consumer.receive(1000, TimeUnit.MILLISECONDS);
        Assert.assertNull(message2);
    }
}
