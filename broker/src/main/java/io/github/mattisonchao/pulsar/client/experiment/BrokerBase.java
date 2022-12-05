package io.github.mattisonchao.pulsar.client.experiment;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;
import java.util.function.Consumer;


@Slf4j
public class BrokerBase {
    private PulsarContainer pulsar;
    protected String brokerURL = "pulsar://127.0.0.1:6650";
    protected String webServiceURL;

    protected void init(String name, Consumer<PulsarContainer> conf) {
        DockerImageName imageName = DockerImageName.parse(name);
        // --------- Start pulsar standalone container
        this.pulsar = new PulsarContainer(imageName);
        conf.accept(pulsar);
        pulsar.start();
        log.info("====== Start Apache Pulsar success. ======");
        this.brokerURL = pulsar.getPulsarBrokerUrl();
        this.webServiceURL = pulsar.getHttpServiceUrl();
    }

    protected void init(String name) {
        init(name, pulsar -> { });
    }


    protected void cleanup() {
        if (pulsar != null) {
            pulsar.close();
            log.info("====== Close Apache Pulsar success. ======");
        }
    }
}
