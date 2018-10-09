/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.openmessaging.benchmark.driver.nakadi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import nakadi.*;
import org.apache.bookkeeper.stats.StatsLogger;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static io.openmessaging.benchmark.driver.nakadi.NakadiEvent.BENCHMARK_EVENT;

public class NakadiBenchmarkDriver implements BenchmarkDriver {
    private NakadiClient nakadiClient;
    private final Properties commonProperties = new Properties();
    private final Properties producerProperties = new Properties();


    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        Config config = mapper.readValue(configurationFile, Config.class);
        commonProperties.load(new StringReader(config.commonConfig));
        producerProperties.load(new StringReader(config.producerConfig));

        URI nakadiUri = URI.create(commonProperties.getProperty("nakadiBaseUri"));
        nakadiClient = NakadiClient.newBuilder()
                .baseURI(nakadiUri)
                .build();
    }

    @Override
    public String getTopicNamePrefix() {
        return BENCHMARK_EVENT;
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        return CompletableFuture.runAsync(() -> {
            try {
                EventTypeResource eventTypes = nakadiClient.resources().eventTypes();

                ObjectMapper mapper = new ObjectMapper();
                JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);

                JsonSchema schema = schemaGen.generateSchema(NakadiEvent.class);

                String jsonSchema = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
                System.out.println(jsonSchema);

                // create a new event type, using an escaped string for the schema
                EventType nakadiEventType = new EventType()
                        .category(EventType.Category.data)
                        .name(topic)
                        .owningApplication("open-messaging")
                        .partitionStrategy(EventType.PARTITION_RANDOM)
                        .enrichmentStrategy(EventType.ENRICHMENT_METADATA)
                        .partitionKeyFields("key")
                        .cleanupPolicy("delete")
                        .eventTypeStatistics(new EventTypeStatistics(3000*60, 1024, 10, 10))
                        .schema(new EventTypeSchema().schema(
                                jsonSchema));
                Response response = eventTypes.create(nakadiEventType);
                System.out.println(response.responseBody().asString());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        return CompletableFuture.completedFuture(new NakadiBenchmarkProducer(nakadiClient, topic, producerProperties));
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        return CompletableFuture.completedFuture(new NakadiBenchmarkConsumer(nakadiClient, topic, consumerCallback));
    }

    @Override
    public void close() throws Exception {
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

}
