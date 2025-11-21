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

import java.util.Objects;

final class MetadataConsistencyContext implements ConsistencyContext {
    private static final long serialVersionUID = 1L;

    private final String clusterId;
    private final long offset;

    private MetadataConsistencyContext(String clusterId, long offset) {
        this.clusterId = clusterId;
        this.offset = offset;
    }

    @Override
    public ConsistencyContext later(ConsistencyContext other) {
        if (this == other) return this;
        Objects.requireNonNull(other, "The other consistency context is null");
        if (getClass() != other.getClass()) {
            throw new IllegalArgumentException(
                String.format(
                    "Attempting to compare different consistency context %s and %s",
                    getClass().getSimpleName(),
                    other.getClass().getSimpleName()
                )
            );
        }
        MetadataConsistencyContext that = (MetadataConsistencyContext) other;

        if (this.isUnknown()) {
            /* this is the unknown consistency context, assume that the other consistency context
             * is greater
             */
            return that;
        } else if (that.isUnknown()) {
            // that is the unkonwn consistency context, this must be greater
            return this;
        } else if (clusterId.equals(that.clusterId)) {
            if (offset >= that.offset) {
                return this;
            } else {
                return that;
            }
        } else {
            throw new IllegalArgumentException(
                String.format(
                    "The new %s is invalid compared to the stored %s",
                    that,
                    this
                )
            );
        }
    }

    @Override
    public boolean isUnknown() {
        return clusterId == null;
    }

    @Override
    public String toString() {
        return String.format(
            "MetadataConsistencyContext(clusterId=%s, offset=%s)",
            clusterId,
            offset
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MetadataConsistencyContext that = (MetadataConsistencyContext) obj;
        return Objects.equals(clusterId, that.clusterId) &&
            offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId, offset);
    }

    public static ConsistencyContext of(String clusterId, long offset) {
        Objects.requireNonNull(clusterId, "Cluster id cannot be null");

        if (offset < 0) {
            throw new IllegalArgumentException(String.format("Offset (%s) cannot be negative", offset));
        }

        return new MetadataConsistencyContext(clusterId, offset);
    }

    public static ConsistencyContext unknown() {
        return new MetadataConsistencyContext(null, -1);
    }
}
