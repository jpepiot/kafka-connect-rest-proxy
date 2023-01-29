package com.athena.kafka.connect.restproxy.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.athena.kafka.connect.restproxy.models.EventRecord;

public class KafkaRestProxyClient {

    private static final Logger log = LoggerFactory.getLogger(HttpSourceTask.class);
    private String baseUrl;
    private ObjectMapper mapper = new ObjectMapper();

    public KafkaRestProxyClient(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public Boolean createConsumer(String consumerName, String instanceName)
            throws Exception {

        HttpClient client = HttpClient.newBuilder().build();
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("\"name\": \"" + instanceName + "\",");
        builder.append("\"format\": \"binary\",");
        builder.append("\"auto.offset.reset\": \"earliest\",");
        builder.append("\"auto.commit.enable\": \"true\"");
        builder.append("}");

        // Check if already elxists
        HttpRequest httpRequest = HttpRequest
                .newBuilder(new URI(baseUrl + "/consumers/" + consumerName))
                .setHeader("Content-Type", "application/vnd.kafka.v2+json")
                .POST(BodyPublishers.ofString(builder.toString())).build();

        HttpResponse<String> response = client.send(httpRequest, BodyHandlers.ofString());
        if (response.statusCode() != 200 && response.statusCode() != 409) {
            throw new Exception("POST consumer -> " + response.body());
        }

        log.info("Create consumer response : " + response.body());

        return true;
    }

    public void createSubscription(String topic, String consumerName, String instanceName)
            throws URISyntaxException, IOException, InterruptedException, Exception {

        StringBuilder builder = new StringBuilder();
        builder = new StringBuilder();
        builder.append("{");
        builder.append("\"topics\": [");
        builder.append("\"" + topic + "\"");
        builder.append("]}");

        final HttpRequest httpRequest = HttpRequest
                .newBuilder(new URI(baseUrl + "/consumers/" + consumerName
                        + "/instances/" + instanceName + "/subscription"))
                .header("Content-Type", "application/vnd.kafka.json.v2+json")
                .POST(BodyPublishers.ofString(builder.toString()))
                .build();

        HttpClient client = HttpClient.newBuilder().build();
        HttpResponse<Void> response = client.send(httpRequest, BodyHandlers.discarding());
        if (response.statusCode() != 204) {
            throw new Exception("Could not subscribe: " + response.statusCode());
        }
    }

    public void deleteSubscription(String consumerName, String instanceName)
            throws URISyntaxException, IOException, InterruptedException, Exception {

        final HttpRequest httpRequest = HttpRequest
                .newBuilder(new URI(baseUrl + "/consumers/" + consumerName
                        + "/instances/" + instanceName + "/subscription"))
                .DELETE()
                .build();

        HttpClient client = HttpClient.newBuilder().build();
        HttpResponse<Void> response = client.send(httpRequest, BodyHandlers.discarding());
        if (response.statusCode() != 204) {
            throw new Exception("Could not delete subscription: " + response.statusCode());
        }
    }

    public EventRecord[] getRecords(String consumerName, String instanceName, int maxBytes)
            throws URISyntaxException, IOException, InterruptedException, Exception {

        final HttpRequest httpRequest = HttpRequest
                .newBuilder(new URI(baseUrl + "/consumers/" + consumerName
                        + "/instances/" + instanceName + "/records?max_bytes=" + maxBytes))
                .setHeader("Accept", "application/vnd.kafka.binary.v2+json")
                //.setHeader("Accept", "application/vnd.kafka.jsonschema.v2+json")
                .GET()
                .build();

        final HttpClient client = HttpClient.newBuilder().build();
        final HttpResponse<String> response = client.send(httpRequest, BodyHandlers.ofString());

        log.info(response.body());
        
        if (response.statusCode() != 200) {
            // {"error_code":40403,"message":"Consumer instance not found."}
            log.error("GET records -> " + response.body());
            throw new Exception("Could not get records: " + response.statusCode() + ", " + response.body());
        }

        return mapper.readValue(response.body(), EventRecord[].class);
    }
}
