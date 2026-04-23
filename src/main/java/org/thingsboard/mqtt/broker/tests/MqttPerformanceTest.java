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
package org.thingsboard.mqtt.broker.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.ResourceLeakDetector;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.thingsboard.mqtt.broker.config.TestRunConfiguration;
import org.thingsboard.mqtt.broker.data.ClientCredentialsType;
import org.thingsboard.mqtt.broker.data.Message;
import org.thingsboard.mqtt.broker.data.PersistentClientType;
import org.thingsboard.mqtt.broker.data.PublisherGroup;
import org.thingsboard.mqtt.broker.data.SubscriberAnalysisResult;
import org.thingsboard.mqtt.broker.data.SubscriberGroup;
import org.thingsboard.mqtt.broker.data.dto.MqttClientCredentialsDto;
import org.thingsboard.mqtt.broker.service.DummyClientService;
import org.thingsboard.mqtt.broker.service.PayloadGenerator;
import org.thingsboard.mqtt.broker.service.PersistedMqttClientService;
import org.thingsboard.mqtt.broker.service.PublishStats;
import org.thingsboard.mqtt.broker.service.PublisherService;
import org.thingsboard.mqtt.broker.service.SubscribeStats;
import org.thingsboard.mqtt.broker.service.SubscriberService;
import org.thingsboard.mqtt.broker.service.TbBrokerRestService;
import org.thingsboard.mqtt.broker.service.orchestration.ClusterSynchronizer;
import org.thingsboard.mqtt.broker.service.orchestration.TestRestService;
import org.thingsboard.mqtt.broker.util.ThingsBoardThreadFactory;
import org.thingsboard.mqtt.broker.util.ValidationUtil;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class MqttPerformanceTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    public static final String DEFAULT_USER_NAME = "tbmq";
    public static final String DEFAULT_PASSWORD = "tbmq";

    private final DummyClientService dummyClientService;
    private final SubscriberService subscriberService;
    private final PublisherService publisherService;
    private final PersistedMqttClientService persistedMqttClientService;
    private final TestRunConfiguration testRunConfiguration;
    private final PayloadGenerator payloadGenerator;
    private final TestRestService testRestService;
    private final ClusterSynchronizer clusterSynchronizer;

    @Autowired(required = false)
    private TbBrokerRestService tbBrokerRestService;

    private ScheduledExecutorService latencyScheduler;

    @Value("${mqtt.netty.leak_detector_level}")
    private String leakDetectorLevel;
    @Value("${test-run.wait-time-after-clients-disconnect-ms}")
    private int waitTimeAfterDisconnectsMs;
    @Value("${test-run.wait-time-clients-closed-ms}")
    private int waitTimeClientsClosedMs;
    @Value("${test-run.publisher-warmup-sleep:5000}")
    private long publisherWarmUpSleepMs;
    @Value("${test-run.max_total_clients_per_iteration:0}")
    private int maxTotalClientsPerIteration;
    @Value("${stats.period:60}")
    private int period;

    @PostConstruct
    public void init() throws Exception {
        ValidationUtil.validateSubscriberGroups(testRunConfiguration.getSubscribersConfig());
        ValidationUtil.validatePublisherGroups(testRunConfiguration.getPublishersConfig());

        latencyScheduler = Executors.newSingleThreadScheduledExecutor(ThingsBoardThreadFactory.forName("latency-scheduler"));

        log.info("Setting resource leak detector level to {}", leakDetectorLevel);
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.valueOf(leakDetectorLevel.toUpperCase()));
    }

    public void runTest() throws Exception {
        log.info("Start performance test.");

        printTestRunConfiguration();

//        final UUID defaultCredentialsId = createDefaultMqttCredentials();

        log.info("Start clear persisted Sessions.");
//        persistedMqttClientService.clearPersistedSessions();
//        persistedMqttClientService.removeApplicationClients();
        Thread.sleep(2000);
//        persistedMqttClientService.initApplicationClients();

        SubscribeStats subscribeStats = new SubscribeStats(new DescriptiveStatistics(), new DescriptiveStatistics());

        subscriberService.connectSubscribers(subscribeStats);

        subscriberService.subscribe(subscribeStats);
        DescriptiveStatistics generalLatencyStats = subscribeStats.getLatencyStats();
        DescriptiveStatistics msgProcessingLatencyStats = subscribeStats.getMsgProcessingLatencyStats();

        publisherService.connectPublishers();

        dummyClientService.connectDummyClients();

        Thread.sleep(2000);
        publisherService.warmUpPublishers();
        Thread.sleep(publisherWarmUpSleepMs);

        boolean orchestratorNotified = testRestService.notifyNodeIsReady();
        if (orchestratorNotified) {
            clusterSynchronizer.awaitClusterReady();
        }

        log.info("Start msg publishing.");
        PublishStats publishStats = publisherService.startPublishing();
        DescriptiveStatistics acknowledgedStats = publishStats.getPublishAcknowledgedStats();
        DescriptiveStatistics sentStats = publishStats.getPublishSentLatencyStats();

        latencyScheduler.scheduleAtFixedRate(() -> {
            printLatencyStats(generalLatencyStats, msgProcessingLatencyStats, acknowledgedStats, sentStats);
            clearStats(generalLatencyStats, msgProcessingLatencyStats, acknowledgedStats, sentStats);
        }, period, period, TimeUnit.SECONDS);

        Thread.sleep(TimeUnit.SECONDS.toMillis(testRunConfiguration.getSecondsToRun() + testRunConfiguration.getAdditionalSecondsToWait()));

        subscriberService.disconnectSubscribers();
        publisherService.disconnectPublishers();
        dummyClientService.disconnectDummyClients();

        // wait for all MQTT clients to close
        Thread.sleep(waitTimeAfterDisconnectsMs);

//        persistedMqttClientService.clearPersistedSessions();

        SubscriberAnalysisResult analysisResult = subscriberService.analyzeReceivedMessages();
        log.info("Messages stats: lost messages - {}, duplicated messages - {}.",
                analysisResult.getLostMessages(), analysisResult.getDuplicatedMessages()
        );
        printLatencyStats(generalLatencyStats, msgProcessingLatencyStats, acknowledgedStats, sentStats);

        publisherService.printDebugPublishersStats();
        subscriberService.printDebugSubscribersStats();

        // wait for all MQTT clients to close
        Thread.sleep(waitTimeClientsClosedMs);
        persistedMqttClientService.removeApplicationClients();

//        removeDefaultCredentials(defaultCredentialsId);

        log.info("Performance test finished.");
    }

    private void printLatencyStats(DescriptiveStatistics generalLatencyStats, DescriptiveStatistics msgProcessingLatencyStats,
                                   DescriptiveStatistics acknowledgedStats, DescriptiveStatistics sentStats) {
        log.info("Latency stats: median - {}, avg - {}, max - {}, min - {}, 95th - {}, total received messages - {}, " +
                        "publish sent messages - {}, publish sent latency avg - {}, publish sent latency max - {}, " +
                        "publish acknowledged messages - {}, publish acknowledged latency median - {}, publish acknowledged latency avg - {}, publish acknowledged latency max - {}, " +
                        "publish acknowledged latency 95th - {}, msg processing latency max - {}.",
                generalLatencyStats.getPercentile(50),
                generalLatencyStats.getMean(), generalLatencyStats.getMax(),
                generalLatencyStats.getMin(), generalLatencyStats.getPercentile(95),
                generalLatencyStats.getN(),
                sentStats.getN(), sentStats.getMean(), sentStats.getMax(),
                acknowledgedStats.getN(), acknowledgedStats.getPercentile(50), acknowledgedStats.getMean(), acknowledgedStats.getMax(),
                acknowledgedStats.getPercentile(95), msgProcessingLatencyStats.getMax()
        );
    }

    private void clearStats(DescriptiveStatistics generalLatencyStats, DescriptiveStatistics msgProcessingLatencyStats,
                            DescriptiveStatistics acknowledgedStats, DescriptiveStatistics sentStats) {
        generalLatencyStats.clear();
        sentStats.clear();
        acknowledgedStats.clear();
        msgProcessingLatencyStats.clear();
    }

    private UUID createDefaultMqttCredentials() {
        try {
            return UUID.fromString(tbBrokerRestService.createClientCredentials(
                    new MqttClientCredentialsDto(null, DEFAULT_USER_NAME, PersistentClientType.DEVICE, ClientCredentialsType.MQTT_BASIC)
            ));
        } catch (Exception e) {
            log.warn("[{}][{}] Could not create client credentials!", DEFAULT_USER_NAME, PersistentClientType.DEVICE, e);
        }
        return null;
    }

    private void removeDefaultCredentials(UUID id) {
        if (id != null) {
            tbBrokerRestService.removeClientCredentials(id);
        }
    }

    private void printTestRunConfiguration() throws Exception {
        List<PublisherGroup> publisherGroups = testRunConfiguration.getPublishersConfig();
        List<SubscriberGroup> subscriberGroups = testRunConfiguration.getSubscribersConfig();
        int totalPublishers = publisherGroups.stream().mapToInt(PublisherGroup::getPublishers).sum();
        int nonPersistedSubscribers = subscriberGroups.stream()
                .filter(subscriberGroup -> subscriberGroup.getPersistentSessionInfo() == null)
                .mapToInt(SubscriberGroup::getSubscribers)
                .sum();
        int persistedApplicationsSubscribers = subscriberGroups.stream()
                .filter(subscriberGroup -> subscriberGroup.getPersistentSessionInfo() != null
                        && subscriberGroup.getPersistentSessionInfo().getClientType() == PersistentClientType.APPLICATION)
                .mapToInt(SubscriberGroup::getSubscribers)
                .sum();
        int persistedDevicesSubscribers = subscriberGroups.stream()
                .filter(subscriberGroup -> subscriberGroup.getPersistentSessionInfo() != null
                        && subscriberGroup.getPersistentSessionInfo().getClientType() == PersistentClientType.DEVICE)
                .mapToInt(SubscriberGroup::getSubscribers)
                .sum();
        int totalPublishedMessages;
        int totalExpectedReceivedMessages;
        if (maxTotalClientsPerIteration > 0) {
            totalPublishedMessages = maxTotalClientsPerIteration * testRunConfiguration.getTotalPublisherMessagesCount();
            totalExpectedReceivedMessages = totalPublishedMessages;
        } else {
            totalPublishedMessages = totalPublishers * testRunConfiguration.getTotalPublisherMessagesCount();
            totalExpectedReceivedMessages = subscriberService.calculateTotalExpectedReceivedMessages();
        }
        Message randomMsg = new Message(System.currentTimeMillis(), true, payloadGenerator.generatePayload());
        log.info("Test run info: publishers - {}, non-persistent subscribers - {}, regular persistent subscribers - {}, " +
                        "'APPLICATION' persistent subscribers - {}, dummy client connections - {}, " +
                        "publisher QoS - {}, subscriber QoS - {}, max messages per second - {}, " +
                        "run time - {}s, total published messages - {}, expected total received messages - {}, " +
                        "msg bytes size - {}",
                totalPublishers, nonPersistedSubscribers, persistedDevicesSubscribers,
                persistedApplicationsSubscribers, testRunConfiguration.getNumberOfDummyClients(),
                testRunConfiguration.getPublisherQoS(), testRunConfiguration.getSubscriberQoS(), testRunConfiguration.getMaxMessagesPerPublisherPerSecond(),
                testRunConfiguration.getSecondsToRun(), totalPublishedMessages, totalExpectedReceivedMessages,
                mapper.writeValueAsBytes(randomMsg).length);
    }

    @PreDestroy
    public void destroy() {
        if (latencyScheduler != null) {
            latencyScheduler.shutdownNow();
        }
    }
}
