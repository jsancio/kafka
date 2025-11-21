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

/**
 * An object that can be used to read and write the latest metadata consistency context for a Kafka
 * cluster.
 *
 * A {@code ConsistencyContextStore} exposes two operations. The method {@code storeLatest} can be
 * used to store the latest metadata consistency context. The method {@code read} can be used to
 * read the latest metadata consistent context.
 */
public interface ConsistencyContextStore {
    /**
     * Method for storing the latest consistency context.
     *
     * Overrides the stored metadata {@code ConsistencyContext} if the provided
     * {@code consistencyContext} is more up to date than the stored metadata consistency context.
     * Otherwise the already stored metadata {@code ConsistencyContext} is kept.
     *
     * Returns {@code true} if the stored metadata consistency context was updated. Otherwise, it
     * returns {@code false}.
     *
     * @param consistencyContext the new consistency context to attempt store
     * @return true the consistency context was updated, otherwise returns false
     * @throws IllegalArgumentException if the cluster id do not match
     */
    boolean storeLatest(ConsistencyContext consistencyContext);

    /**
     * Method for reading the latest consistency context.
     *
     * @return the currently stored metadata consistency context
     */
    ConsistencyContext read();

    /**
     * Creates and returns an {@code ConsistencyContextStore} object with
     * {@code consistencyContext} as the starting value.
     *
     * @param consistencyContext the initial value for the consisteny context store
     * @return a memory backed consistency context store
     */
    public static ConsistencyContextStore of(ConsistencyContext consistencyContext) {
        return MemoryConsistencyContextStore.of(consistencyContext);
    }

    /**
     * Creates and returns an {@code ConsistencyContextStore} object with no initial value.
     *
     * @return a memory backed consistency context store
     */
    public static ConsistencyContextStore empty() {
        return MemoryConsistencyContextStore.of(ConsistencyContext.unknown());
    }

    /**
     * Legacy metadata consistency context.
     *
     * This metadata consistency context store achieves eventual consistency context by always
     * returning the empty consistency context.
     *
     * @return a consistency context store that always the empty consistency context
     */
    public static ConsistencyContextStore eventuallyConsistent() {
        return LegacyConsistencyContextStore.singleton();
    }
}
