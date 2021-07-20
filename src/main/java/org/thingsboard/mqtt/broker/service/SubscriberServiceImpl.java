/**
 * Copyright © 2016-2021 The Thingsboard Authors
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
package org.thingsboard.mqtt.broker.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Future;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.thingsboard.mqtt.broker.client.mqtt.MqttClient;
import org.thingsboard.mqtt.broker.config.TestRunClusterConfig;
import org.thingsboard.mqtt.broker.config.TestRunConfiguration;
import org.thingsboard.mqtt.broker.data.Message;
import org.thingsboard.mqtt.broker.data.PublisherGroup;
import org.thingsboard.mqtt.broker.data.SubscriberAnalysisResult;
import org.thingsboard.mqtt.broker.data.SubscriberGroup;
import org.thingsboard.mqtt.broker.data.SubscriberInfo;
import org.thingsboard.mqtt.broker.tests.MqttPerformanceTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@RequiredArgsConstructor
public class SubscriberServiceImpl implements SubscriberService {
    private final ObjectMapper mapper = new ObjectMapper();

    private final ClientInitializer clientInitializer;
    private final TestRunConfiguration testRunConfiguration;
    private final ClientIdService clientIdService;
    private final TestRunClusterConfig testRunClusterConfig;
    private final ClusterProcessService clusterProcessService;

    private final Map<String, SubscriberInfo> subscriberInfos = new ConcurrentHashMap<>();

    @Override
    public void connectSubscribers() {
        List<PreConnectedSubscriberInfo> preConnectedSubscriberInfos = new ArrayList<>();
        int currentSubscriberId = 0;
        for (SubscriberGroup subscriberGroup : testRunConfiguration.getSubscribersConfig()) {
            for (int i = 0; i < subscriberGroup.getSubscribers(); i++) {
                if (currentSubscriberId++ % testRunClusterConfig.getParallelTestsCount() == testRunClusterConfig.getSequentialNumber()) {
                    preConnectedSubscriberInfos.add(new PreConnectedSubscriberInfo(subscriberGroup, i));
                }
            }
        }

        clusterProcessService.process("SUBSCRIBERS_CONNECT", preConnectedSubscriberInfos, (latch, preConnectedSubscriberInfo) -> {
            int subscriberIndex = preConnectedSubscriberInfo.getSubscriberIndex();
            SubscriberGroup subscriberGroup = preConnectedSubscriberInfo.getSubscriberGroup();

            String clientId = clientIdService.createSubscriberClientId(subscriberGroup, subscriberIndex);
            boolean cleanSession = subscriberGroup.getPersistentSessionInfo() == null;
            MqttClient subClient = clientInitializer.createClient(clientId, cleanSession, (s, mqttMessageByteBuf, receivedTime) -> {
                try {
                    byte[] mqttMessageBytes = toBytes(mqttMessageByteBuf);
                    Message message = mapper.readValue(mqttMessageBytes, Message.class);
                    log.warn("Received persisted message for the test run ID - {} and time {}", message.getTestRunId(), message.getCreateTime());
                } catch (Exception e) {
                    log.error("[{}] Failed to process msg", clientId);
                }
            });
            clientInitializer.connectClient(subClient).addListener(future -> {
                if (!future.isSuccess()) {
                    log.warn("[{}] Failed to connect subscriber", clientId);
                    subClient.disconnect();
                } else {
                    subscriberInfos.put(clientId, new SubscriberInfo(subClient, subscriberIndex, clientId, new AtomicInteger(0), subscriberGroup,
                            subscriberGroup.isDebugEnabled() ? new DescriptiveStatistics() : null));
                }
                latch.countDown();
            });
        });
    }

    @Override
    public SubscribeStats subscribe() {
        DescriptiveStatistics generalLatencyStats = new DescriptiveStatistics();
        DescriptiveStatistics msgProcessingLatencyStats = new DescriptiveStatistics();
        ConcurrentHashMap<Long, AtomicLong> oldMessagesByTestRunId = new ConcurrentHashMap<>();

        clusterProcessService.process("SUBSCRIBERS_SUBSCRIBE", new ArrayList<>(subscriberInfos.values()), (latch, subscriberInfo) -> {
            Future<Void> subscribeFuture = subscriberInfo.getSubscriber().on(subscriberInfo.getSubscriberGroup().getTopicFilter(), (topic, mqttMessageByteBuf, receivedTime) -> {
                try {
                    long now = System.currentTimeMillis();
                    byte[] mqttMessageBytes = toBytes(mqttMessageByteBuf);
                    Message message = mapper.readValue(mqttMessageBytes, Message.class);
                    long msgTestRunId = message.getTestRunId() != null ? message.getTestRunId() : -1L;
                    if (message.isWarmUpMsg()) {
                        return;
                    }
                    if (MqttPerformanceTest.TEST_RUN_ID != msgTestRunId) {
                        oldMessagesByTestRunId.computeIfAbsent(msgTestRunId, id -> new AtomicLong(0)).incrementAndGet();
                    }
                    long msgLatency = receivedTime - message.getCreateTime();
                    generalLatencyStats.addValue(msgLatency);
                    msgProcessingLatencyStats.addValue(now - receivedTime);
                    if (subscriberInfo.getLatencyStats() != null) {
                        subscriberInfo.getLatencyStats().addValue(msgLatency);
                        log.debug("[{}] Received msg with time {}", subscriberInfo.getClientId(), message.getCreateTime());
                    }
                    subscriberInfo.getReceivedMsgs().incrementAndGet();
                } catch (Exception e) {
                    log.error("[{}] Failed to process msg", subscriberInfo.getId());
                }
            }, testRunConfiguration.getSubscriberQoS());

            subscribeFuture.addListener(future -> {
                latch.countDown();
            });
        });

        return new SubscribeStats(generalLatencyStats, msgProcessingLatencyStats, oldMessagesByTestRunId);
    }

    @Override
    public void disconnectSubscribers() {
        log.info("Disconnecting subscribers.");
        for (SubscriberInfo subscriberInfo : subscriberInfos.values()) {
            try {
                subscriberInfo.getSubscriber().disconnect();
            } catch (Exception e) {
                log.error("[{}] Failed to disconnect subscriber", subscriberInfo.getClientId());
            }
        }
    }

    @Override
    public SubscriberAnalysisResult analyzeReceivedMessages() {
        Map<Integer, PublisherGroup> publisherGroupsById = testRunConfiguration.getPublishersConfig().stream()
                .collect(Collectors.toMap(PublisherGroup::getId, Function.identity()));
        int lostMessages = 0;
        int duplicatedMessages = 0;
        for (SubscriberInfo subscriberInfo : subscriberInfos.values()) {
            int expectedReceivedMsgs = getSubscriberExpectedReceivedMsgs(testRunConfiguration.getTotalPublisherMessagesCount(), publisherGroupsById, subscriberInfo.getSubscriberGroup());
            int actualReceivedMsgs = subscriberInfo.getReceivedMsgs().get();
            if (actualReceivedMsgs != expectedReceivedMsgs) {
                log.trace("[{}] Expected messages count - {}, actual messages count - {}",
                        subscriberInfo.getClientId(), expectedReceivedMsgs, actualReceivedMsgs);
                if (expectedReceivedMsgs > actualReceivedMsgs) {
                    lostMessages += expectedReceivedMsgs - actualReceivedMsgs;
                } else {
                    duplicatedMessages += actualReceivedMsgs - expectedReceivedMsgs;
                }
            }
        }
        return SubscriberAnalysisResult.builder()
                .lostMessages(lostMessages)
                .duplicatedMessages(duplicatedMessages)
                .build();
    }

    @Override
    public int calculateTotalExpectedReceivedMessages() {
        Map<Integer, PublisherGroup> publisherGroupsById = testRunConfiguration.getPublishersConfig().stream()
                .collect(Collectors.toMap(PublisherGroup::getId, Function.identity()));
        return testRunConfiguration.getSubscribersConfig().stream()
                .mapToInt(subscriberGroup -> subscriberGroup.getSubscribers() * getSubscriberExpectedReceivedMsgs(testRunConfiguration.getTotalPublisherMessagesCount(), publisherGroupsById, subscriberGroup))
                .sum();
    }

    @Override
    public void printDebugSubscribersStats() {
        for (SubscriberInfo subscriberInfo : subscriberInfos.values()) {
            DescriptiveStatistics stats = subscriberInfo.getLatencyStats();
            if (stats != null) {
                log.info("[{}] Subscriber general latency: messages - {}, median - {}, 95 percentile - {}, max - {}.",
                        subscriberInfo.getClientId(), stats.getN(), stats.getMean(), stats.getPercentile(95), stats.getMax());
            }
        }
    }

    private int getSubscriberExpectedReceivedMsgs(int totalProducerMessagesCount, Map<Integer, PublisherGroup> publisherGroupsById, SubscriberGroup subscriberGroup) {
        return subscriberGroup.getExpectedPublisherGroups().stream()
                .map(publisherGroupsById::get)
                .filter(Objects::nonNull)
                .map(PublisherGroup::getPublishers)
                .mapToInt(Integer::intValue)
                .map(publishersInGroup -> publishersInGroup * totalProducerMessagesCount)
                .sum();
    }


    private static byte[] toBytes(ByteBuf inbound) {
        byte[] bytes = new byte[inbound.readableBytes()];
        int readerIndex = inbound.readerIndex();
        inbound.getBytes(readerIndex, bytes);
        return bytes;
    }

    @Getter
    @AllArgsConstructor
    private static class PreConnectedSubscriberInfo {
        private final SubscriberGroup subscriberGroup;
        private final int subscriberIndex;
    }
}
