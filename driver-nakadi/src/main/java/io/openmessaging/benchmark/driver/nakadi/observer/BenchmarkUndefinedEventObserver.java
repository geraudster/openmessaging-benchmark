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

package io.openmessaging.benchmark.driver.nakadi.observer;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import nakadi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Helper class to log data events from a stream with Map data.
 */
public class BenchmarkUndefinedEventObserver
        extends StreamObserverBackPressure<UndefinedEventMapped<Map<String, Object>>> {

    private static final Logger logger =
            LoggerFactory.getLogger(BenchmarkUndefinedEventObserver.class);

    private final ConsumerCallback consumerCallback;

    public BenchmarkUndefinedEventObserver(ConsumerCallback consumerCallback) {
        this.consumerCallback = consumerCallback;
    }

    @Override
    public void onStart() {
        logger.info("onStart");
    }

    @Override
    public void onStop() {
        logger.info("onStop");
    }

    @Override
    public void onCompleted() {
        logger.info("onCompleted {}", Thread.currentThread().getName());
    }

    @Override
    public void onError(Throwable e) {
        logger.info("onError {} {}", e.getMessage(), Thread.currentThread().getName());
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void onNext(StreamBatchRecord<UndefinedEventMapped<Map<String, Object>>> record) {
        final StreamOffsetObserver offsetObserver = record.streamOffsetObserver();
        final StreamBatch<UndefinedEventMapped<Map<String, Object>>> batch = record.streamBatch();
        final StreamCursorContext cursor = record.streamCursorContext();

        if (batch.isEmpty()) {
            logger.info("partition: {} empty batch", cursor.cursor().partition());
        } else {
            batch.events().forEach(event -> consumerCallback.messageReceived(
                    getBytes(event),
                    getPublishTimestamp(event))
            );
        }
        offsetObserver.onNext(record.streamCursorContext());
    }

    private long getPublishTimestamp(UndefinedEventMapped<Map<String, Object>> event) {
        return System.currentTimeMillis();
    }

    private byte[] getBytes(UndefinedEventMapped<Map<String, Object>> event) {
        List<Double> byteList = (ArrayList<Double>) event.data().get("payload");
        byte[] data = new byte[byteList.size()];
        for (int i = 0; i < data.length; i++) {
            data[i] = byteList.get(i).byteValue();
        }
        return data;
    }
}
