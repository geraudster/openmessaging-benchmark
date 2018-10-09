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

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import nakadi.*;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.openmessaging.benchmark.driver.nakadi.NakadiEvent.BENCHMARK_EVENT;

public class NakadiBenchmarkProducer implements BenchmarkProducer {

    private final ExecutorService executor;
    private final String topic;
    private final EventResource events;
    private final boolean async;

    public NakadiBenchmarkProducer(NakadiClient nakadiClient, String topic, Properties producerConfig) {
        this.executor = Executors.newFixedThreadPool(Integer.parseInt(producerConfig.getProperty("nbThreads")));
        this.topic = topic;
        this.async = Boolean.valueOf(producerConfig.getProperty("async"));
        events = nakadiClient.resources().events();
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        final CompletableFuture<Void> future;
        if(async) {
            future = CompletableFuture.supplyAsync(() -> {
                sendEvent(key, payload);
                return null;
            }, executor);
        } else {
            future = new CompletableFuture<>();
            Response response = sendEvent(key, payload);
            if(response.statusCode() == 200) {
                future.complete(null);
            } else {
                future.completeExceptionally(new RuntimeException("Cannot send message"));
            }
        }

        return future;
    }

    private Response sendEvent(Optional<String> key, byte[] payload) {
        EventMetadata metadata = new EventMetadata().withEid().withOccurredAt().flowId("PRODUCER_TEST");

        DataChangeEvent<NakadiEvent> data = new DataChangeEvent<NakadiEvent>()
                .metadata(metadata)
                .op(DataChangeEvent.Op.C)
                .dataType(BENCHMARK_EVENT)
                .data(new NakadiEvent(
                        key.orElse(""),
                        payload));

        return events.send(topic, data);
    }

    @Override
    public void close() throws Exception {
    }
}
