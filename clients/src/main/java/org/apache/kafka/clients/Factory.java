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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.metadata.ConsistencyContext;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

// TODO: document this..
public final class Factory {
    private final ConsistencyContextStore latestConsistencyContext;

    // TODO: document this...
    Factory(ConsistencyContext latestConsistencyContext) {
        this.latestConsistencyContext = ConsistencyContextStore.of(latestConsistencyContext);
    }

    // TODO: document this...
    public Admin admin(Map<String, Object> config) {
        // TODO: implement this...
        return null;
    }

    // TODO: document this..
    public <K, V> Producer<K, V> producer(
        Map<String, Object> config,
        Serializer<K> keySerializer,
        Serializer<V> valueSerializer
    ) {
        // TODO: implement this...
        return null;
    }

    // TODO: document this..
    public <K, V> Consumer<K, V> consumer(
        Map<String, Object> config,
        Deserializer<K> keyDeserializer,
        Deserializer<V> valueDeserializer
    ) {
        // TODO: implement this...
        return null;
    }
}
