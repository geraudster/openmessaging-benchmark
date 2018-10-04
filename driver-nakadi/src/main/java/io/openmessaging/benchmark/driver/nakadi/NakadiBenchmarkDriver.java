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
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static io.openmessaging.benchmark.driver.nakadi.NakadiEvent.BENCHMARK_EVENT;

public class NakadiBenchmarkDriver implements BenchmarkDriver {


    private static final URI NAKADI_URI = URI.create("http://localhost:8080");
    private NakadiClient nakadiClient;
    private String topic;

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        nakadiClient = NakadiClient.newBuilder()
                .baseURI(NAKADI_URI)
                .build();
    }

    @Override
    public String getTopicNamePrefix() {
        return BENCHMARK_EVENT;
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        this.topic = topic;
        return CompletableFuture.runAsync(() -> {
            try {
                EventTypeResource eventTypes = nakadiClient.resources().eventTypes();

                ObjectMapper mapper = new ObjectMapper();
                JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);

                JsonSchema schema = schemaGen.generateSchema(NakadiEvent.class);

                String jsonSchema = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
                System.out.println(jsonSchema);
                // create a new event type, using an escaped string for the schema
                EventType requisitions = new EventType()
                        .category(EventType.Category.data)
                        .name(topic)
                        .owningApplication("open-messaging")
                        .partitionStrategy(EventType.PARTITION_HASH)
                        .enrichmentStrategy(EventType.ENRICHMENT_METADATA)
                        .partitionKeyFields("key")
                        .cleanupPolicy("delete")
                        .schema(new EventTypeSchema().schema(
                                jsonSchema));
                Response response = eventTypes.create(requisitions);
                System.out.println(response.responseBody().asString());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        return CompletableFuture.completedFuture(new NakadiBenchmarkProducer(nakadiClient, topic));
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        return CompletableFuture.completedFuture(new NakadiBenchmarkConsumer(nakadiClient, topic, consumerCallback));
    }

    @Override
    public void close() throws Exception {

    }
}
