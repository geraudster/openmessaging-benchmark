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
import nakadi.DataChangeEvent;
import nakadi.EventMetadata;
import nakadi.EventResource;
import nakadi.NakadiClient;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static io.openmessaging.benchmark.driver.nakadi.NakadiEvent.BENCHMARK_EVENT;

public class NakadiBenchmarkProducer implements BenchmarkProducer {

    private final String topic;
    private final EventResource eventResource;
    private final LinkedBlockingQueue<AbstractMap.SimpleImmutableEntry<CompletableFuture<Void>, DataChangeEvent<NakadiEvent>>> queue;
    private final int pollingDelayInMs;

    private boolean closing = false;

    public NakadiBenchmarkProducer(NakadiClient nakadiClient, String topic, Properties producerConfig) {
        this.topic = topic;
        this.queue = new LinkedBlockingQueue<>(Integer.parseInt(producerConfig.getProperty("maxQueueSize")));
        this.pollingDelayInMs = Integer.parseInt(producerConfig.getProperty("pollingDelayInMs"));
        this.eventResource = nakadiClient.resources().events();
        Thread nakadiBatchProducerThread = new Thread(new NakadiBatchProducer(queue, producerConfig));
        nakadiBatchProducerThread.start();
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        while(queue.remainingCapacity() <= 0) {
            try {
                Thread.sleep(pollingDelayInMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        queue.add(new AbstractMap.SimpleImmutableEntry<>(future, convertToDataChangeEvent(new NakadiEvent(key.orElse(""), payload))));

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
    }

    class NakadiBatchProducer implements Runnable {

        private final LinkedBlockingQueue<AbstractMap.SimpleImmutableEntry<CompletableFuture<Void>, DataChangeEvent<NakadiEvent>>> queue;
        private final int batchSize;

        NakadiBatchProducer(LinkedBlockingQueue<AbstractMap.SimpleImmutableEntry<CompletableFuture<Void>, DataChangeEvent<NakadiEvent>>> queue, Properties producerConfig) {
            this.queue = queue;
            this.batchSize = Integer.parseInt(producerConfig.getProperty("batchSize"));
        }

        @Override
        public void run() {
            final List<AbstractMap.SimpleImmutableEntry<CompletableFuture<Void>, DataChangeEvent<NakadiEvent>>> tuples = new ArrayList<>();
            AbstractMap.SimpleImmutableEntry<CompletableFuture<Void>, DataChangeEvent<NakadiEvent>> tuple;
            while (!closing) {
                for (int i = 0; i < batchSize; i++) {
                    tuple = queue.poll();
                    if(tuple != null) {
                        tuples.add(tuple);
                    }
                }
                if(!tuples.isEmpty()) {
                    List<DataChangeEvent<NakadiEvent>> events = tuples.stream()
                            .map(AbstractMap.SimpleImmutableEntry::getValue)
                            .collect(Collectors.toList());

                    eventResource.send(topic, events);
                    tuples.stream()
                            .map(AbstractMap.SimpleImmutableEntry::getKey)
                            .forEach(voidCompletableFuture -> voidCompletableFuture.complete(null));
                    tuples.clear();
                }
            }
        }
    }
}
