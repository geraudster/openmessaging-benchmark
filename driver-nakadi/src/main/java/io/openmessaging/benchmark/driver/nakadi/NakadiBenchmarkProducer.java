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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.openmessaging.benchmark.driver.nakadi.NakadiEvent.BENCHMARK_EVENT;

public class NakadiBenchmarkProducer implements BenchmarkProducer {

    private final ExecutorService executor;
    private final NakadiClient nakadiClient;
    private final String topic;

    public NakadiBenchmarkProducer(NakadiClient nakadiClient, String topic) {
        this.executor = Executors.newSingleThreadExecutor();
        this.nakadiClient = nakadiClient;
        this.topic = topic;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        this.executor.submit(() -> {
            System.out.println(topic);
            Response response = nakadiClient.resources().events().send(topic,
                    new DataChangeEvent<NakadiEvent>()
                            .metadata(EventMetadata.newPreparedEventMetadata())
                            .op(DataChangeEvent.Op.C)
                            .dataType(BENCHMARK_EVENT)
                            .data(
                            new NakadiEvent(
                                    key.orElse(""),
                                    payload,
                                    System.currentTimeMillis())));
            System.out.println(response.statusCode());
            System.out.println(response.responseBody().asString());
            future.complete(null);
            return null;
        });

        return future;
    }

    @Override
    public void close() throws Exception {
    }
}
