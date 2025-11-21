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
package org.apache.kafka.common.metadata;

import java.io.Serializable;

/**
 * An object that represent the metadata consistency context for a given Kafka cluster.
 *
 * A {@code ConsistencyContext} exposes two operations. The method {@code later} compares
 * two consistency context and return the more up to date context. The method {@code isUnknown}
 * returns true if the object represents an unknown metadata consistency context.
 */
public interface ConsistencyContext extends Serializable {
    /**
     * Compares two consistency context and returns the more up to date consistency context.
     *
     * The returned consistency context will have the greater of the two offsets. If the cluster
     * id do not match an {@code IllegalArgumentException} is thrown.
     *
     * @param other the consistency context to compare
     * @return the more up to date consistency context
     * @throws IllegalArgumentException if neither consistency context is empty and the cluster id
     *     do not match
     */
    public ConsistencyContext later(ConsistencyContext other);

    /**
     * Returns true is the consistency context is unknown.
     */
    public boolean isUnknown();

    /**
     * Returns the empty consistency context.
     *
     * This is the default consistency context when the value is unknown.
     */
    public static ConsistencyContext unknown() {
        return MetadataConsistencyContext.unknown();
    }

    /**
     * Returns a consistency context describing the given cluster id and offset.
     *
     * @param clusterId the cluster id
     * @param offset the metadata offset
     * @return an consistency context representing the cluster id and offset
     * @throws IllegalArgumentException if offset is negative
     * @throws NullPointerException if the cluster id is null
     */
    public static ConsistencyContext of(String clusterId, long offset) {
        return MetadataConsistencyContext.of(clusterId, offset);
    }
}
