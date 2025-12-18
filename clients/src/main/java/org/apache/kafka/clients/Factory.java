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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Object for creating Kafka clients with a shared consistency.
 *
 * This object allows the user to create Admin clients, Producer clients and Consumer clients with
 * a shared consistency.
 *
 * For example, if you would like to create an Admin client to create ACLs and a topic, and have
 * the producer and consumer to see a consistent view of the cluster metadata then use the same
 * factory to create all of the associated clients.
 *
 * This object implements three important menthods. The method {@code admin} can be used to create
 * Admin clients. The method {@code producer} can be used to create Producer clients. The
 * method {@code consumer} can be used to create Consumer clients.
 */
public final class Factory {
    private final ConsistencyContextStore store;

    /**
     * Creates a Factory object.
     *
     * @param store the store for storing the latest consistency context
     */
    Factory(ConsistencyContextStore store) {
        this.store = store;
    }

    /**
     * Creates an Admin client.
     *
     * @param config the admin client configuration
     */
    public Admin admin(Map<String, Object> config) {
        // TODO: implement this...
        return null;
    }

    /**
     * Creates a Producer client.
     *
     * @param config the producer configuration
     * @param keySerializer the serializer for the key
     * @param ValueSerializer the serializer for the value
     */
    public <K, V> Producer<K, V> producer(
        Map<String, Object> config,
        Serializer<K> keySerializer,
        Serializer<V> valueSerializer
    ) {
        // TODO: implement this...
        return null;
    }

    /**
     * Creates a Consumer clients.
     *
     * @param config the consumer configuration
     * @param keyDeserializer the deserializer for the key
     * @param valueDeserializer the deserializer for the value
     */
    public <K, V> Consumer<K, V> consumer(
        Map<String, Object> config,
        Deserializer<K> keyDeserializer,
        Deserializer<V> valueDeserializer
    ) {
        // TODO: implement this...
        return null;
    }
}
