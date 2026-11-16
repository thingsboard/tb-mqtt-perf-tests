/**
 * Copyright © 2016-2024 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.mqtt.broker.client.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class ReceivedMsgProcessorImplTest {

    private static final int THREADS = 4;
    private static final int SUBSCRIBERS = 8;
    private static final int MESSAGES_PER_SUBSCRIBER = 10_000;

    private ReceivedMsgProcessorImpl processor;

    @AfterEach
    void tearDown() {
        if (processor != null) {
            processor.destroy();
        }
    }

    @Test
    void givenSubscribersSpreadOverHandlers_whenMessagesReceived_thenEachSubscriberIsInOrderOnOneThreadAndSeveralThreadsWork() throws Exception {
        List<String> subscriberIds = IntStream.rangeClosed(1, SUBSCRIBERS).mapToObj(i -> "sub-" + i).toList();
        Set<Integer> targetThreads = subscriberIds.stream()
                .map(id -> Math.floorMod(id.hashCode(), THREADS))
                .collect(Collectors.toSet());
        assertThat(targetThreads).as("the chosen ids must map to at least 2 of the %d handler threads", THREADS).hasSizeGreaterThanOrEqualTo(2);

        Map<String, List<Integer>> receivedBySubscriber = new ConcurrentHashMap<>();
        Map<String, Set<String>> threadsBySubscriber = new ConcurrentHashMap<>();
        Set<String> allThreads = ConcurrentHashMap.newKeySet();
        CountDownLatch allProcessed = new CountDownLatch(SUBSCRIBERS * MESSAGES_PER_SUBSCRIBER);

        processor = new ReceivedMsgProcessorImpl(60_000L, THREADS);
        processor.init();

        for (int seq = 0; seq < MESSAGES_PER_SUBSCRIBER; seq++) {
            for (String subscriberId : subscriberIds) {
                BiConsumer<MqttPublishMessage, Long> handler = (msg, receivedTime) -> {
                    String threadName = Thread.currentThread().getName();
                    allThreads.add(threadName);
                    threadsBySubscriber.computeIfAbsent(subscriberId, k -> ConcurrentHashMap.newKeySet()).add(threadName);
                    // a subscriber is served by a single thread, so its list is never written concurrently
                    receivedBySubscriber.computeIfAbsent(subscriberId, k -> new ArrayList<>())
                            .add(msg.payload().getInt(msg.payload().readerIndex()));
                    allProcessed.countDown();
                };
                processor.processIncomingMessage(subscriberId, publishWithSequence(seq), handler);
            }
        }

        assertThat(allProcessed.await(60, TimeUnit.SECONDS)).as("all messages are processed in time").isTrue();

        for (String subscriberId : subscriberIds) {
            assertThat(receivedBySubscriber.get(subscriberId))
                    .as("messages of %s", subscriberId)
                    .containsExactlyElementsOf(IntStream.range(0, MESSAGES_PER_SUBSCRIBER).boxed().toList());
            assertThat(threadsBySubscriber.get(subscriberId)).as("threads serving %s", subscriberId).hasSize(1);
        }
        assertThat(allThreads).hasSizeGreaterThanOrEqualTo(2);
    }

    private static MqttPublishMessage publishWithSequence(int seq) {
        ByteBuf payload = Unpooled.buffer(Integer.BYTES).writeInt(seq);
        return new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttPublishVariableHeader("test/topic", 1),
                payload);
    }
}
