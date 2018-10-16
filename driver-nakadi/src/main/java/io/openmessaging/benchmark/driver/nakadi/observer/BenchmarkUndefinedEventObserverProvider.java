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

import java.util.Map;

/**
 * Helper class to provide {@link DataChangeEvent} with Map data.
 */
public class BenchmarkUndefinedEventObserverProvider
        implements StreamObserverProvider<UndefinedEventMapped<Map<String, Object>>> {

    private final ConsumerCallback consumerCallback;

    public BenchmarkUndefinedEventObserverProvider(ConsumerCallback consumerCallback) {
        this.consumerCallback = consumerCallback;
    }

    @Override
    public StreamObserver<UndefinedEventMapped<Map<String, Object>>> createStreamObserver() {
        return new BenchmarkUndefinedEventObserver(consumerCallback);
    }

    @Override
    public TypeLiteral<UndefinedEventMapped<Map<String, Object>>> typeLiteral() {
        return TypeLiterals.OF_UNDEFINED_MAP;
    }
}
