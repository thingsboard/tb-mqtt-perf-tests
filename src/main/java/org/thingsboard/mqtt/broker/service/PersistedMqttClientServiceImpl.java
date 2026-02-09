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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.thingsboard.mqtt.broker.client.mqtt.MqttClient;
import org.thingsboard.mqtt.broker.config.TestRunClusterConfig;
import org.thingsboard.mqtt.broker.config.TestRunConfiguration;
import org.thingsboard.mqtt.broker.data.ClientCredentialsType;
import org.thingsboard.mqtt.broker.data.PersistentClientType;
import org.thingsboard.mqtt.broker.data.PreConnectedSubscriberInfo;
import org.thingsboard.mqtt.broker.data.ShortMqttClientCredentials;
import org.thingsboard.mqtt.broker.data.dto.MqttClientCredentialsDto;
import org.thingsboard.mqtt.broker.tests.MqttPerformanceTest;
import org.thingsboard.mqtt.broker.util.CallbackUtil;
import org.thingsboard.mqtt.broker.util.TestClusterUtil;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class PersistedMqttClientServiceImpl implements PersistedMqttClientService {

    @Value("${test-run.clear-persisted-sessions-wait-time}")
    private int waitTime;

    @Autowired(required = false)
    private TbBrokerRestService tbBrokerRestService;

    private final ClientInitializer clientInitializer;
    private final TestRunConfiguration testRunConfiguration;
    private final TestRunClusterConfig testRunClusterConfig;
    private final ClientIdService clientIdService;

    @Override
    public void initApplicationClients() {
        if (tbBrokerRestService == null) {
            return;
        }

        List<PreConnectedSubscriberInfo> applicationNodeSubscribers = getAppSubscribers();
        if (CollectionUtils.isEmpty(applicationNodeSubscribers)) return;

        List<ShortMqttClientCredentials> allClientCredentials = tbBrokerRestService.getAllClientCredentials();

        log.info("Initializing {} {} subscribers.", applicationNodeSubscribers.size(), PersistentClientType.APPLICATION);
        for (PreConnectedSubscriberInfo preConnectedSubscriberInfo : applicationNodeSubscribers) {
            String clientId = clientIdService.createSubscriberClientId(preConnectedSubscriberInfo.getSubscriberGroup(), preConnectedSubscriberInfo.getSubscriberIndex());
            ShortMqttClientCredentials credentials = findCredentialsByClientId(allClientCredentials, clientId);
            if (credentials == null) {
                try {
                    tbBrokerRestService.createClientCredentials(
                            new MqttClientCredentialsDto(clientId, clientId, PersistentClientType.APPLICATION, ClientCredentialsType.MQTT_BASIC)
                    );
                } catch (Exception ignored) {
                }
            } else if (credentials.getClientType() == PersistentClientType.DEVICE) {
                log.error("Client with ID {} exists with {} client type.", credentials, PersistentClientType.DEVICE);
                throw new RuntimeException("Client with ID " + clientId + " exists with " + PersistentClientType.DEVICE + " type");
            }
        }
    }

    private List<PreConnectedSubscriberInfo> getAppSubscribers() {
        List<PreConnectedSubscriberInfo> nodeSubscribers = TestClusterUtil.getTestNodeSubscribers(testRunConfiguration, testRunClusterConfig);
        return nodeSubscribers.stream()
                .filter(preConnectedSubscriberInfo -> preConnectedSubscriberInfo.getSubscriberGroup().getPersistentSessionInfo() != null)
                .filter(preConnectedSubscriberInfo -> preConnectedSubscriberInfo.getSubscriberGroup().getPersistentSessionInfo().getClientType() == PersistentClientType.APPLICATION)
                .collect(Collectors.toList());
    }

    private ShortMqttClientCredentials findCredentialsByClientId(List<ShortMqttClientCredentials> allClientCredentials, String clientId) {
        return allClientCredentials
                .stream()
                .filter(credentials -> credentials.getName().equals(clientId))
                .findFirst()
                .orElse(null);
    }

    @Override
    public void clearPersistedSessions() throws InterruptedException {
        log.info("Start clear persisted Sessions.");
        List<PreConnectedSubscriberInfo> nodeSubscribers = TestClusterUtil.getTestNodeSubscribers(testRunConfiguration, testRunClusterConfig);
        List<PreConnectedSubscriberInfo> persistedNodeSubscribers = nodeSubscribers.stream()
                .filter(preConnectedSubscriberInfo -> preConnectedSubscriberInfo.getSubscriberGroup().getPersistentSessionInfo() != null)
                .toList();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        CountDownLatch countDownLatch = new CountDownLatch(persistedNodeSubscribers.size());
        for (PreConnectedSubscriberInfo preConnectedSubscriberInfo : persistedNodeSubscribers) {
            String clientId = clientIdService.createSubscriberClientId(preConnectedSubscriberInfo.getSubscriberGroup(), preConnectedSubscriberInfo.getSubscriberIndex());
            MqttClient mqttClient = clientInitializer.createClient(clientId, MqttPerformanceTest.DEFAULT_USER_NAME, true);
            clientInitializer.connectClient(CallbackUtil.createConnectCallback(
                            connectResult -> {
                                mqttClient.disconnectAndClose();
                                countDownLatch.countDown();
                            }, t -> {
                                log.warn("[{}] Failed to clear persisted session", clientId);
                                mqttClient.disconnectAndClose();
                                countDownLatch.countDown();
                            }
                    ),
                    mqttClient);
        }

        var result = countDownLatch.await(waitTime, TimeUnit.SECONDS);
        log.info("The result of await processing for clear persisted sessions is: {}", result);
        stopWatch.stop();
        log.info("Clearing {} persisted sessions took {} ms", persistedNodeSubscribers.size(), stopWatch.getTime());
    }

    @Override
    public void removeApplicationClients() {
        if (tbBrokerRestService == null) {
            return;
        }

        List<PreConnectedSubscriberInfo> applicationNodeSubscribers = getAppSubscribers();
        if (CollectionUtils.isEmpty(applicationNodeSubscribers)) return;

        List<ShortMqttClientCredentials> allClientCredentials = tbBrokerRestService.getAllClientCredentials();

        log.info("Removing {} {} subscribers.", applicationNodeSubscribers.size(), PersistentClientType.APPLICATION);
        for (PreConnectedSubscriberInfo preConnectedSubscriberInfo : applicationNodeSubscribers) {
            String clientId = clientIdService.createSubscriberClientId(preConnectedSubscriberInfo.getSubscriberGroup(), preConnectedSubscriberInfo.getSubscriberIndex());
            ShortMqttClientCredentials credentials = findCredentialsByClientId(allClientCredentials, clientId);
            if (credentials == null) {
                return;
            }
            try {
                tbBrokerRestService.removeClientCredentials(credentials.getId());
            } catch (Exception e) {
                log.warn("[{}] Failed to remove {} client", clientId, PersistentClientType.APPLICATION, e);
            }
        }
    }
}
