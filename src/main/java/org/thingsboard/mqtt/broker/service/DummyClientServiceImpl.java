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

import com.google.common.collect.HashMultimap;
import io.netty.handler.codec.mqtt.MqttQoS;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.thingsboard.mqtt.broker.client.mqtt.MqttClient;
import org.thingsboard.mqtt.broker.client.mqtt.MqttSubscription;
import org.thingsboard.mqtt.broker.config.TestRunClusterConfig;
import org.thingsboard.mqtt.broker.config.TestRunConfiguration;
import org.thingsboard.mqtt.broker.tests.MqttPerformanceTest;
import org.thingsboard.mqtt.broker.util.CallbackUtil;
import org.thingsboard.mqtt.broker.util.ThingsBoardThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@RequiredArgsConstructor
public class DummyClientServiceImpl implements DummyClientService {

    private final ClientInitializer clientInitializer;
    private final TestRunConfiguration testRunConfiguration;
    private final ClientIdService clientIdService;
    private final TestRunClusterConfig testRunClusterConfig;
    private final ClusterProcessService clusterProcessService;
    private final HostPortService hostPortService;

    private Map<String, MqttClient> dummyClients;

    private ScheduledExecutorService scheduler;

    @PostConstruct
    public void init() {
        scheduler = Executors.newSingleThreadScheduledExecutor(ThingsBoardThreadFactory.forName("dummy-scheduler"));
        // Run frequency: Every 2 seconds is enough for 1-minute resolution metrics
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (CollectionUtils.isEmpty(dummyClients)) {
                    return;
                }
                // 1. Calculate "Global Mood" based on time (Sine Wave)
                // Cycle duration: ~10 minutes (600,000 ms)
                // value goes from -1.0 (Worst) to +1.0 (Best)
                double timeFactor = 2.0 * Math.PI * (System.currentTimeMillis() % 600000) / 600000.0;
                double mood = Math.sin(timeFactor);

                // 2. Batch size: 3 to 8 clients per tick
                int batchSize = ThreadLocalRandom.current().nextInt(3, 9);
                List<MqttClient> clients = new ArrayList<>(dummyClients.values());

                for (int i = 0; i < batchSize; i++) {
                    MqttClient client = clients.get(ThreadLocalRandom.current().nextInt(clients.size()));
                    String clientId = client.getClientConfig().getClientId();
                    boolean isConnected = client.isConnected();

                    int action = -1; // -1 means do nothing

                    // 3. DECISION LOGIC BASED ON "MOOD"
                    // If Mood is HIGH (> 0): Prefer Connecting & Subscribing (Growth Phase)
                    // If Mood is LOW (< 0): Prefer Disconnecting & Unsubscribing (Failure Phase)

                    double roll = ThreadLocalRandom.current().nextDouble(); // 0.0 to 1.0

                    if (isConnected) {
                        // Actions: 0=Disconnect, 2=Subscribe, 3=Unsubscribe
                        if (mood < -0.3) {
                            // "Bad Mood": High chance to Disconnect or Unsubscribe
                            if (roll < 0.15) action = 0;      // 15% Disconnect
                            else if (roll < 0.4) action = 3;  // 25% Unsubscribe
                            else if (roll < 0.5) action = 2;  // 10% Subscribe (still happens occasionally)
                        } else {
                            // "Good Mood": Mostly Stable, some Subscribing
                            if (roll < 0.02) action = 0;      // 2% Disconnect (Accidents happen)
                            else if (roll < 0.2) action = 2;  // 18% Subscribe
                            else if (roll < 0.3) action = 3;  // 10% Unsubscribe
                        }
                    } else {
                        // Actions: 1=Connect
                        if (mood > 0.2) {
                            // "Good Mood": Aggressive Recovery
                            if (roll < 0.6) action = 1;       // 60% chance to Reconnect
                        } else {
                            // "Bad Mood": Slow Recovery
                            if (roll < 0.1) action = 1;       // Only 10% chance to Reconnect
                        }
                    }

                    // 4. EXECUTE ACTION
                    switch (action) {
                        case 0: // DISCONNECT
                            log.debug("[{}] Simulating device failure (Disconnect)", clientId);
                            client.disconnect();
                            break;

                        case 1: // CONNECT
                            log.debug("[{}] Simulating device recovery (Connect)", clientId);
                            client.connect(
                                    CallbackUtil.createConnectCallback(res -> {
                                    }, err -> client.disconnectAndClose()),
                                    hostPortService.getHostPort().getHost()
                            );
                            break;

                        case 2: // SUBSCRIBE
                            String topic = TopicDictionary.getRandomTopic(clientId);
                            int qos = ThreadLocalRandom.current().nextInt(0, 3);
                            client.on(topic, (t, p, time) -> {
                            }, CallbackUtil.createCallback(() -> {
                            }, __ -> {
                            }), MqttQoS.valueOf(qos));
                            break;

                        case 3: // UNSUBSCRIBE
                            HashMultimap<String, MqttSubscription> subscriptions = client.getSubscriptions();
                            if (!subscriptions.isEmpty()) {
                                List<String> activeTopics = new ArrayList<>(subscriptions.keySet());
                                client.off(activeTopics.get(ThreadLocalRandom.current().nextInt(activeTopics.size())));
                            }
                            break;
                    }
                }
            } catch (Exception e) {
                log.error("Error in lifecycle simulation", e);
            }
        }, 5000, 2000, TimeUnit.MILLISECONDS); // Run every 2 seconds
    }

    @Override
    public void connectDummyClients() {
        this.dummyClients = new ConcurrentHashMap<>();
        DescriptiveStatistics connectionStats = new DescriptiveStatistics();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        List<Integer> preConnectedDummyIndexes = new ArrayList<>();
        int currentDummyId = 0;
        for (int i = 0; i < testRunConfiguration.getNumberOfDummyClients(); i++) {
            if (currentDummyId++ % testRunClusterConfig.getParallelTestsCount() == testRunClusterConfig.getSequentialNumber()) {
                preConnectedDummyIndexes.add(i);
            }
        }

        clusterProcessService.process("DUMMIES_CONNECT", preConnectedDummyIndexes, (latch, dummyId) -> {
            String clientId = clientIdService.createDummyClientId(dummyId);
            MqttClient dummyClient = clientInitializer.createClient(clientId, MqttPerformanceTest.DEFAULT_USER_NAME);
            long connectionStart = System.currentTimeMillis();
            clientInitializer.connectClient(CallbackUtil.createConnectCallback(connectResult -> {
                        dummyClients.put(clientId, dummyClient);
                        connectionStats.addValue(System.currentTimeMillis() - connectionStart);

                        dummyClient.on(
                                TopicDictionary.getRandomTopic(clientId),
                                (topic, payload, receiveTime) -> {
                                },
                                CallbackUtil.createCallback(() -> {
                                }, throwable -> {
                                })
                        );

                        latch.countDown();
                    }, t -> {
                        log.warn("Failed to connect dummy client {}", clientId);
                        dummyClient.disconnectAndClose();
                        latch.countDown();
                    }),
                    dummyClient);
        });

        stopWatch.stop();
        int totalNodeDummies = testRunConfiguration.getNumberOfDummyClients() / testRunClusterConfig.getParallelTestsCount()
                + (testRunConfiguration.getNumberOfDummyClients() % testRunClusterConfig.getParallelTestsCount() > testRunClusterConfig.getSequentialNumber() ? 1 : 0);
        log.info("Connecting {} dummy clients took {} ms, avg connection time - {}, max connection time - {}, 95 percentile connection time - {}.",
                totalNodeDummies, stopWatch.getTime(), connectionStats.getMean(),
                connectionStats.getMax(), connectionStats.getPercentile(95.0));
    }

    @Override
    public void disconnectDummyClients() {
        log.info("Disconnecting dummy clients.");
        int clientIndex = 0;
        for (MqttClient dummyClient : dummyClients.values()) {
            try {
                dummyClient.disconnectAndClose();
            } catch (Exception e) {
                log.error("[{}] Failed to disconnect dummy client", clientIndex);
            }
            clientIndex++;
        }
        dummyClients = null;
    }

    @PreDestroy
    public void destroy() {
        if (dummyClients != null) {
            disconnectDummyClients();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
