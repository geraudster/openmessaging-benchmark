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
import nakadi.StreamProcessor;

public class NakadiBenchmarkConsumer implements BenchmarkConsumer {

    private final StreamProcessor processor;

    public NakadiBenchmarkConsumer(NakadiClient nakadiClient, String topic, ConsumerCallback callback) {

        StreamConfiguration sc = new StreamConfiguration()
                .eventTypeName(topic);

        processor = nakadiClient.resources().streamBuilder()
                .streamConfiguration(sc)
                .streamObserverFactory(new BenchmarkDataChangeEventObserverProvider(callback))
                .build();

        processor.start();
        System.out.println("Processor started");

    }

    @Override
    public void close() throws Exception {
        processor.stop();
    }

}
