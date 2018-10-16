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
import io.openmessaging.benchmark.driver.nakadi.adapter.EventAdapter;
import nakadi.Event;
import nakadi.EventResource;
import nakadi.NakadiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.WorkQueueProcessor;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class NakadiBenchmarkProducer implements BenchmarkProducer {
    private static final Logger logger = LoggerFactory.getLogger(NakadiBenchmarkProducer.class);

    private final EventResource eventResource;

    private final WorkQueueProcessor<Event<NakadiEvent>> processor;
    private final FluxSink<Event<NakadiEvent>> sink;
    private final EventAdapter eventAdapter;

    public NakadiBenchmarkProducer(NakadiClient nakadiClient, String topic, Properties producerConfig, EventAdapter eventAdapter) {
        this.eventResource = nakadiClient.resources().events();
        int batchSize = Integer.parseInt(producerConfig.getProperty("batchSize"));
        this.eventAdapter = eventAdapter;

        processor = WorkQueueProcessor.create();
        sink = processor.sink(FluxSink.OverflowStrategy.DROP);

        Flux.from(processor)
//                .buffer(batchSize)
                .map(events -> eventResource.send(topic, events))
                .subscribe();

        logger.info("Producer started");
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        Event<NakadiEvent> event = eventAdapter.convertToEvent(new NakadiEvent(key.orElse(""), payload));

        sink.next(event);

        future.complete(null);
        return future;
    }

    @Override
    public void close() throws Exception {
        sink.complete();
    }
}
