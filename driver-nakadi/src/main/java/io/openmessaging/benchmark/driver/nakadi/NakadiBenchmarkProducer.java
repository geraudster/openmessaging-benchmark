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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static io.openmessaging.benchmark.driver.nakadi.NakadiEvent.BENCHMARK_EVENT;

public class NakadiBenchmarkProducer implements BenchmarkProducer {
    private static final Logger logger = LoggerFactory.getLogger(NakadiBenchmarkProducer.class);

    private final String topic;
    private final EventResource eventResource;

    private boolean closing = false;
    private final EmitterProcessor<DataChangeEvent<NakadiEvent>> processor;
    private final FluxSink<DataChangeEvent<NakadiEvent>> sink;

    public NakadiBenchmarkProducer(NakadiClient nakadiClient, String topic, Properties producerConfig) {
        this.topic = topic;
        this.eventResource = nakadiClient.resources().events();
        int batchSize = Integer.parseInt(producerConfig.getProperty("batchSize"));

        processor = EmitterProcessor.create(batchSize);
        sink = processor.sink(FluxSink.OverflowStrategy.DROP);

        Flux<DataChangeEvent<NakadiEvent>> from = Flux.from(processor);
        from.onBackpressureBuffer()
                .bufferTimeout(batchSize, Duration.ofMillis(100))
                .map(events -> eventResource.send(topic, events))
                .bufferTimeout(Integer.MAX_VALUE, Duration.ofSeconds(1))
                .doOnNext(latencies -> logger.info("Sent messages :     " + latencies.size()))
                .subscribe();
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        DataChangeEvent<NakadiEvent> event = convertToDataChangeEvent(new NakadiEvent(key.orElse(""), payload));
        while(sink.requestedFromDownstream() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        sink.next(event);

        future.complete(null);
        return future;
    }

    private DataChangeEvent<NakadiEvent> convertToDataChangeEvent(NakadiEvent nakadiEvent) {
        EventMetadata metadata = new EventMetadata().withEid().withOccurredAt().flowId("PRODUCER_TEST");

        return new DataChangeEvent<NakadiEvent>()
                .metadata(metadata)
                .op(DataChangeEvent.Op.C)
                .dataType(BENCHMARK_EVENT)
                .data(nakadiEvent);
    }

    @Override
    public void close() throws Exception {
        closing = true;
        sink.complete();
    }
}
