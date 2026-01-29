/**
 * Copyright © 2016-2026 The Thingsboard Authors
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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.thingsboard.mqtt.broker.client.mqtt.MqttClient;
import org.thingsboard.mqtt.broker.config.TestRunClusterConfig;
import org.thingsboard.mqtt.broker.config.TestRunConfiguration;
import org.thingsboard.mqtt.broker.data.Message;
import org.thingsboard.mqtt.broker.data.PersistentClientType;
import org.thingsboard.mqtt.broker.data.PreConnectedSubscriberInfo;
import org.thingsboard.mqtt.broker.data.PublisherGroup;
import org.thingsboard.mqtt.broker.data.SubscriberAnalysisResult;
import org.thingsboard.mqtt.broker.data.SubscriberGroup;
import org.thingsboard.mqtt.broker.data.SubscriberInfo;
import org.thingsboard.mqtt.broker.tests.MqttPerformanceTest;
import org.thingsboard.mqtt.broker.util.CallbackUtil;
import org.thingsboard.mqtt.broker.util.TestClusterUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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

    @Value("${stats.enabled:true}")
    private boolean statsEnabled;
    @Value("${test-run.max_total_clients_per_iteration:0}")
    private int maxTotalClientsPerIteration;

    @Override
    public void connectSubscribers(SubscribeStats subscribeStats) {
        List<PreConnectedSubscriberInfo> preConnectedSubscriberInfos = TestClusterUtil.getTestNodeSubscribers(testRunConfiguration, testRunClusterConfig);
        log.debug("Found {} preConnectedSubscriberInfos", preConnectedSubscriberInfos);

        clusterProcessService.process("SUBSCRIBERS_CONNECT", preConnectedSubscriberInfos, (latch, preConnectedSubscriberInfo) -> {
            int subscriberIndex = preConnectedSubscriberInfo.getSubscriberIndex();
            SubscriberGroup subscriberGroup = preConnectedSubscriberInfo.getSubscriberGroup();

            String clientId = clientIdService.createSubscriberClientId(subscriberGroup, subscriberIndex);
            boolean cleanSession = subscriberGroup.getPersistentSessionInfo() == null;
            SubscriberInfo subscriberInfo = new SubscriberInfo(null, subscriberIndex, clientId, new AtomicInteger(0), subscriberGroup,
                    subscriberGroup.isDebugEnabled() ? new DescriptiveStatistics() : null);

            MqttClient subClient;
            if (subscriberGroup.getPersistentSessionInfo() != null && PersistentClientType.APPLICATION == subscriberGroup.getPersistentSessionInfo().getClientType()) {
                subClient = getClient(clientId, MqttPerformanceTest.APP_USER_NAME, cleanSession, subscribeStats, subscriberInfo);
            } else {
                subClient = getClient(clientId, MqttPerformanceTest.DEFAULT_USER_NAME, cleanSession, subscribeStats, subscriberInfo);
            }

            clientInitializer.connectClient(CallbackUtil.createConnectCallback(
                            connectResult -> {
                                subscriberInfo.setSubscriber(subClient);
                                subscriberInfos.put(clientId, subscriberInfo);
                                latch.countDown();
                            }, t -> {
                                log.warn("[{}] Failed to connect subscriber", clientId);
                                subClient.disconnectAndClose();
                                latch.countDown();
                            }
                    ),
                    subClient);
        });
    }

    private MqttClient getClient(String clientId, String defaultUserName, boolean cleanSession, SubscribeStats subscribeStats, SubscriberInfo subscriberInfo) {
        return clientInitializer.createClient(
                clientId,
                defaultUserName,
                cleanSession,
                (s, mqttMessageByteBuf, receivedTime) -> processReceivedMsg(subscribeStats, subscriberInfo, mqttMessageByteBuf, receivedTime)
        );
    }

    @Override
    public void subscribe(SubscribeStats subscribeStats) {
        clusterProcessService.process(
                "SUBSCRIBERS_SUBSCRIBE",
                new ArrayList<>(subscriberInfos.values()),
                (latch, subscriberInfo) ->
                        subscriberInfo.getSubscriber().on(
                                subscriberInfo.getSubscriberGroup().getTopicFilter(),
                                (topic, mqttMessageByteBuf, receivedTime) -> processReceivedMsg(subscribeStats, subscriberInfo, mqttMessageByteBuf, receivedTime),
                                CallbackUtil.createCallback(latch::countDown, t -> latch.countDown()),
                                testRunConfiguration.getSubscriberQoS()));
    }

    private void processReceivedMsg(SubscribeStats subscribeStats, SubscriberInfo subscriberInfo, ByteBuf mqttMessageByteBuf, long receivedTime) {
        try {
            long now = System.currentTimeMillis();
            byte[] mqttMessageBytes = toBytes(mqttMessageByteBuf);

            Message message = null;
            try {
                message = mapper.readValue(mqttMessageBytes, Message.class);
            } catch (Exception ignored) {
            }
            if (message == null) {
                return;
            }
            long msgLatency = receivedTime - message.getCreateTime();
            if (statsEnabled) {
                subscribeStats.getLatencyStats().addValue(msgLatency);
                subscribeStats.getMsgProcessingLatencyStats().addValue(now - receivedTime);
            }
            if (subscriberInfo.getLatencyStats() != null) {
                subscriberInfo.getLatencyStats().addValue(msgLatency);
                log.debug("[{}] Received msg with time {}", subscriberInfo.getClientId(), message.getCreateTime());
            }
            subscriberInfo.getTotalReceivedMsgs().incrementAndGet();
        } catch (Exception e) {
            log.error("[{}] Failed to process msg", subscriberInfo.getId(), e);
        }
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
            int expectedReceivedMsgs;
            if (maxTotalClientsPerIteration > 0) {
                expectedReceivedMsgs = maxTotalClientsPerIteration * testRunConfiguration.getSecondsToRun();
            } else {
                expectedReceivedMsgs = getSubscriberExpectedReceivedMsgs(testRunConfiguration.getTotalPublisherMessagesCount(), publisherGroupsById, subscriberInfo.getSubscriberGroup());
            }
            int actualReceivedMsgs = subscriberInfo.getTotalReceivedMsgs().get();
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

}
