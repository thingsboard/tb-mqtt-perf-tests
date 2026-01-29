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
package org.thingsboard.mqtt.broker.config;

import io.netty.handler.codec.mqtt.MqttQoS;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import org.thingsboard.mqtt.broker.data.PersistentClientType;
import org.thingsboard.mqtt.broker.data.PersistentSessionInfo;
import org.thingsboard.mqtt.broker.data.PublisherGroup;
import org.thingsboard.mqtt.broker.data.SubscriberGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Component
@RequiredArgsConstructor
@ConditionalOnExpression("'${test-run.configuration-file:}'==''")
public class DefaultTestRunConfiguration implements TestRunConfiguration {
    private static final String CONFIGURATION_NAME = "Default Configuration";

    @Value("${test-run.default.worker-type:DEFAULT}")
    private String workerTypeStr;

    @Value("${test-run.default.pub-sub-groups-count:100}")
    private int pubSubGroupsCount;

    @Value("${test-run.default.seconds-to-run:30}")
    private int secondsToRun;

    @Value("${test-run.default.additional-seconds-to-wait:10}")
    private int additionalSecondsToWait;

    @Value("${test-run.default.dummy-clients:0}")
    private int dummyClients;

    @Value("${test-run.default.max-msgs-per-publisher-per-second:1}")
    private int maxMsgsPerPublisherPerSecond;

    @Value("${test-run.default.publisher-qos:0}")
    private int publisherQos;

    @Value("${test-run.default.subscriber-qos:0}")
    private int subscriberQos;

    @Value("${test-run.default.min-payload-size:256}")
    private int minPayloadSize;

    @Value("${test-run.default.max-concurrent-operations:1000}")
    private int maxConcurrentOperations;

    private Pair<List<PublisherGroup>, List<SubscriberGroup>> p2pPubSubConfig;

    @PostConstruct
    public void init() {
        WorkerType workerType = WorkerType.valueOf(workerTypeStr);
        p2pPubSubConfig = createPubSubConfig(workerType);
    }

    @Override
    public String getConfigurationName() {
        return CONFIGURATION_NAME;
    }

    @Override
    public List<SubscriberGroup> getSubscribersConfig() {
        return p2pPubSubConfig.getRight();
    }

    @Override
    public List<PublisherGroup> getPublishersConfig() {
        return p2pPubSubConfig.getLeft();
    }

    @Override
    public int getMaxMessagesPerPublisherPerSecond() {
        return maxMsgsPerPublisherPerSecond;
    }

    @Override
    public int getSecondsToRun() {
        return secondsToRun;
    }

    @Override
    public int getAdditionalSecondsToWait() {
        return additionalSecondsToWait;
    }

    @Override
    public int getTotalPublisherMessagesCount() {
        return secondsToRun * maxMsgsPerPublisherPerSecond;
    }

    @Override
    public int getNumberOfDummyClients() {
        return dummyClients;
    }

    @Override
    public MqttQoS getPublisherQoS() {
        return MqttQoS.valueOf(publisherQos);
    }

    @Override
    public MqttQoS getSubscriberQoS() {
        return MqttQoS.valueOf(subscriberQos);
    }

    @Override
    public int getMinPayloadSize() {
        return minPayloadSize;
    }

    @Override
    public List<String> getTelemetryKeys() {
        return Collections.emptyList();
    }

    @Override
    public int getMaxConcurrentOperations() {
        return maxConcurrentOperations;
    }

    private Pair<List<PublisherGroup>, List<SubscriberGroup>> createPubSubConfig(WorkerType workerType) {
        if (workerType == WorkerType.DEFAULT) {
            var persistentSessionInfo = new PersistentSessionInfo(PersistentClientType.DEVICE);
            List<PublisherGroup> publisherGroups = new ArrayList<>();
            List<SubscriberGroup> subscriberGroups = new ArrayList<>();

            for (int groupIndex = 1; groupIndex <= pubSubGroupsCount; groupIndex++) {
                publisherGroups.add(createP2PPublisherGroup(groupIndex));
                subscriberGroups.add(createP2PSubscribeGroup(groupIndex, persistentSessionInfo));
            }
            return new ImmutablePair<>(publisherGroups, subscriberGroups);
        } else if (workerType == WorkerType.PUBLISHER) {
            List<PublisherGroup> publisherGroups = new ArrayList<>();

            for (int groupIndex = 1; groupIndex <= pubSubGroupsCount; groupIndex++) {
                publisherGroups.add(createP2PPublisherGroup(groupIndex));
            }
            return new ImmutablePair<>(publisherGroups, Collections.emptyList());
        } else {
            var persistentSessionInfo = new PersistentSessionInfo(PersistentClientType.DEVICE);
            List<SubscriberGroup> subscriberGroups = new ArrayList<>();

            for (int groupIndex = 1; groupIndex <= pubSubGroupsCount; groupIndex++) {
                subscriberGroups.add(createP2PSubscribeGroup(groupIndex, persistentSessionInfo));
            }
            return new ImmutablePair<>(Collections.emptyList(), subscriberGroups);
        }
    }

    private SubscriberGroup createP2PSubscribeGroup(int groupIndex, PersistentSessionInfo persistentSessionInfo) {
        return new SubscriberGroup(groupIndex, 1, "europe/ua/kyiv/" + groupIndex + "/+", Set.of(groupIndex), persistentSessionInfo);
    }

    private PublisherGroup createP2PPublisherGroup(int groupIndex) {
        return new PublisherGroup(groupIndex, 1, "europe/ua/kyiv/" + groupIndex + "/");
    }
}
