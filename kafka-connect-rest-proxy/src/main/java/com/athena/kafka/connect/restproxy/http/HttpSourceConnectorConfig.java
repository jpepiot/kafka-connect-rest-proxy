package com.athena.kafka.connect.restproxy.http;

import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static com.athena.kafka.connect.restproxy.common.ConfigUtils.breakDownMap;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

@Getter
class HttpSourceConnectorConfig extends AbstractConfig {

    private static final String SINK_TOPIC_CONFIG = "sink.topic";
    private static final String SOURCE_BASE_URL_CONFIG = "source.base.url";
    private static final String SOURCE_OFFSET_INITIAL_CONFIG = "source.offset.initial";
    private static final String SOURCE_CONSUMER_CONFIG = "source.consumer.name";
    private static final String SOURCE_TOPIC_CONFIG = "source.topic";
    private static final String SOURCE_INSTANCE_CONFIG = "source.instance.name";

    private final Map<String, String> initialOffset;

    HttpSourceConnectorConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        initialOffset = breakDownMap(getString(SOURCE_OFFSET_INITIAL_CONFIG));
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(SOURCE_OFFSET_INITIAL_CONFIG, STRING, "", HIGH, "Starting offset")
                .define(SOURCE_BASE_URL_CONFIG, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), HIGH,
                "The source base url")
                .define(SINK_TOPIC_CONFIG, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), HIGH,
                        "The topic to publish data to")
                .define(SOURCE_TOPIC_CONFIG, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), HIGH,
                        "The topic to consume data from")
                .define(SOURCE_CONSUMER_CONFIG, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                        HIGH,
                        "Name of the consumer")
                .define(SOURCE_INSTANCE_CONFIG, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                        HIGH,
                        "Name of the instance");
    }

    public String getSinkTopic() {
        return this.getString(SINK_TOPIC_CONFIG);
    }

    public String getBaseUrl() {
        return this.getString(SOURCE_BASE_URL_CONFIG);
    }

    public String getSourceTopic() {
        return this.getString(SOURCE_TOPIC_CONFIG);
    }

    public String getInstanceName() {
        return this.getString(SOURCE_INSTANCE_CONFIG);
    }

    public String getConsumerName() {
        return this.getString(SOURCE_CONSUMER_CONFIG);
    }
}
