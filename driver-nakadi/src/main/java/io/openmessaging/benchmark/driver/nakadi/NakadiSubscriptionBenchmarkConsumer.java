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

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.nakadi.observer.BenchmarkDataChangeEventObserverProvider;
import nakadi.NakadiClient;
import nakadi.StreamConfiguration;
import nakadi.StreamObserverProvider;
import nakadi.StreamProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class NakadiSubscriptionBenchmarkConsumer implements BenchmarkConsumer {
    private final Logger logger = LoggerFactory.getLogger(NakadiSubscriptionBenchmarkConsumer.class);

    private final StreamProcessor processor;

    public NakadiSubscriptionBenchmarkConsumer(NakadiClient nakadiClient, Properties consumerProperties, StreamObserverProvider streamObserverProvider, String subscriptionId) {
        int batchBufferCount = Integer.parseInt(consumerProperties.getProperty("batchBufferCount"));
        StreamConfiguration sc = new StreamConfiguration()
                .subscriptionId(subscriptionId)
                .batchLimit(batchBufferCount)
                .maxUncommittedEvents(batchBufferCount);

        processor = nakadiClient.resources().streamBuilder()
                .streamConfiguration(sc)
                .streamObserverFactory(streamObserverProvider)
                .build();

        processor.start();
        logger.info("Consumer started");

    }

    @Override
    public void close() throws Exception {
        processor.stop();
    }

}
