/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.metadata.ConsistencyContext;

import java.util.concurrent.atomic.AtomicReference;

final class MemoryConsistencyContextStore implements ConsistencyContextStore {
    private final AtomicReference<ConsistencyContext> latestConsistencyContext;

    private MemoryConsistencyContextStore(ConsistencyContext latestConsistencyContext) {
        this.latestConsistencyContext = new AtomicReference<>(latestConsistencyContext);
    }

    @Override
    public boolean storeLatest(ConsistencyContext consistencyContext) {
        while (true) {
            var oldContext = latestConsistencyContext.get();
            var latest = oldContext.later(consistencyContext);

            if (latest.equals(consistencyContext)) {
                if (latestConsistencyContext.compareAndSet(oldContext, consistencyContext)) {
                    return true;
                } else {
                    // update contention re-apply the update
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public ConsistencyContext read() {
        return latestConsistencyContext.get();
    }

    public static ConsistencyContextStore of(ConsistencyContext consistencyContext) {
        return new MemoryConsistencyContextStore(consistencyContext);
    }
}
