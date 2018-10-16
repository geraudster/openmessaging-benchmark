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

package io.openmessaging.benchmark.driver.nakadi.adapter;

import io.openmessaging.benchmark.driver.nakadi.NakadiEvent;
import nakadi.DataChangeEvent;
import nakadi.Event;
import nakadi.EventMetadata;

public class DataChangeEventAdapter implements EventAdapter {
    public DataChangeEventAdapter() {
    }

    public Event<NakadiEvent> convertToEvent(NakadiEvent nakadiEvent) {
        EventMetadata metadata = new EventMetadata().withEid().withOccurredAt().flowId("PRODUCER_TEST");

        return new DataChangeEvent<NakadiEvent>()
                .metadata(metadata)
                .op(DataChangeEvent.Op.C)
                .dataType(NakadiEvent.BENCHMARK_EVENT)
                .data(nakadiEvent);
    }
}