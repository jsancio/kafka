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
package org.apache.kafka.raft.internals;

import org.apache.kafka.raft.RaftMessage;
import org.apache.kafka.raft.RaftMessageQueue;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockingMessageQueueTest {

    @Test
    public void testOfferAndPoll() {
        var queue = new BlockingMessageQueue();
        assertTrue(queue.isEmpty());
        assertEquals(Optional.empty(), queue.poll(0));

        var message1 = new RaftMessageQueue.QueueEntry(Mockito.mock(RaftMessage.class));
        queue.add(message1);
        assertFalse(queue.isEmpty());
        assertEquals(Optional.of(message1), queue.poll(0));
        assertTrue(queue.isEmpty());

        var message2 = new RaftMessageQueue.QueueEntry(Mockito.mock(RaftMessage.class));
        var message3 = new RaftMessageQueue.QueueEntry(Mockito.mock(RaftMessage.class));
        queue.add(message2);
        queue.add(message3);
        assertFalse(queue.isEmpty());
        assertEquals(Optional.of(message2), queue.poll(0));
        assertEquals(Optional.of(message3), queue.poll(0));

    }

    @Test
    public void testWakeupFromPoll() {
        var queue = new BlockingMessageQueue();
        queue.wakeup();
        assertEquals(Optional.empty(), queue.poll(Long.MAX_VALUE));
    }

    // TODO: any new tests we can write given the change in semantic? E.g. wakeup follow by add doesn't require two poll
}
