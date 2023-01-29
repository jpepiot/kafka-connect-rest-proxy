package com.athena.kafka.connect.restproxy.http;

import static com.athena.kafka.connect.restproxy.common.VersionUtils.getVersion;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.athena.kafka.connect.restproxy.models.EventRecord;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpSourceTask extends SourceTask {

    private final Function<Map<String, String>, HttpSourceConnectorConfig> configFactory;
    private static final Logger log = LoggerFactory.getLogger(HttpSourceTask.class);
    private HttpSourceConnectorConfig config;
    private KafkaRestProxyClient proxyClient;
    private int WAITING_DELAY_MS = 1000;

    private Boolean initialized = false;
    private ObjectMapper mapper = new ObjectMapper();

    HttpSourceTask(Function<Map<String, String>, HttpSourceConnectorConfig> configFactory) {
        this.configFactory = configFactory;
    }

    public HttpSourceTask() {
        this(HttpSourceConnectorConfig::new);
    }

    @Override
    public void start(Map<String, String> settings) {
        HttpSourceConnectorConfig config = configFactory.apply(settings);
        this.config = config;
        proxyClient = new KafkaRestProxyClient(config.getBaseUrl());
        log.info("Start Kafka REST Proxy source task .....");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        int waitingDelay = WAITING_DELAY_MS;
        final ArrayList<SourceRecord> unseenRecords = new ArrayList<>();

        if (!initialized) {
            synchronized (this) {
                initialized = false;
                try {
                    proxyClient.createConsumer(config.getConsumerName(), config.getInstanceName());
                } catch (Exception e) {
                    log.error("POST consumer -> " + e.getMessage());
                    return unseenRecords;
                }

                initialized = true;
            }
        }

        try {
            proxyClient.createSubscription(config.getSourceTopic(), config.getConsumerName(), config.getInstanceName());
        } catch (Exception e) {
            log.error("Create subscription exception : " + e.getMessage());
            initialized = false;
            return unseenRecords;
        }

        try {
            EventRecord[] events = proxyClient.getRecords(config.getConsumerName(), config.getInstanceName(), 10000);
            if (events.length > 0) {
                for (EventRecord event : events) {
                    log.debug("Event record : " + mapper.writeValueAsString(event.getValue()));
                    byte[] decodedBytes = Base64.getDecoder().decode((String) event.getValue());
                    unseenRecords.add(new SourceRecord(offsetPartition(event.getPartition()),
                            offsetValue(event.getOffset()), config.getSinkTopic(), null, null,
                            event.getKey(), null, decodedBytes, System.currentTimeMillis()));
                }
            }

            log.info("Found " + unseenRecords.size() + " events");
        } catch (Exception e) {
            waitingDelay *= 5;
            initialized = false;
            log.error("Get records -> " + e.getMessage());
            return unseenRecords;
        }

        synchronized (this) {
            this.wait(waitingDelay);
        }

        return unseenRecords;
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        // COMMIT offset
        // POST http://localhost:8082/consumers/gp1/instances/c1/offsets
        /*
         * 
         * {
         * "offsets": [
         * {
         * "topic": "test",
         * "partition": 0,
         * "offset": 20
         * }
         * ]
         * }
         */

        log.info("Commit records .....");
    }

    @Override
    public void commit() {
        log.info("Commit .....");
    }

    @Override
    public void stop() {
        log.info("Stopping task .....");
        try {
            proxyClient.deleteSubscription(config.getConsumerName(), config.getInstanceName());
        } catch (Exception e) {
            log.error(e.getMessage());
        }

        log.info("Task stopped");
    }

    @Override
    public String version() {
        return getVersion();
    }

    private Map<String, Integer> offsetPartition(Integer partition) {
        return Collections.singletonMap(PARTITION_FIELD, partition);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    public static final String PARTITION_FIELD = "partition";
    public static final String POSITION_FIELD = "position";
}
