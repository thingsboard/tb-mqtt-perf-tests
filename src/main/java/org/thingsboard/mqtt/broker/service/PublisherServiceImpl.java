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
import com.google.common.collect.Iterables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.mqtt.MqttQoS;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.thingsboard.mqtt.broker.client.mqtt.MqttClient;
import org.thingsboard.mqtt.broker.config.TestRunClusterConfig;
import org.thingsboard.mqtt.broker.config.TestRunConfiguration;
import org.thingsboard.mqtt.broker.data.Message;
import org.thingsboard.mqtt.broker.data.PublisherGroup;
import org.thingsboard.mqtt.broker.data.PublisherInfo;
import org.thingsboard.mqtt.broker.tests.MqttPerformanceTest;
import org.thingsboard.mqtt.broker.util.CallbackUtil;
import org.thingsboard.mqtt.broker.util.ThingsBoardThreadFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@RequiredArgsConstructor
public class PublisherServiceImpl implements PublisherService {

    private final ObjectMapper mapper = new ObjectMapper();

    private final ClientInitializer clientInitializer;
    private final TestRunConfiguration testRunConfiguration;
    private final ClientIdService clientIdService;
    private final TestRunClusterConfig testRunClusterConfig;
    private final PayloadGenerator payloadGenerator;
    private final ClusterProcessService clusterProcessService;

    private final Map<String, PublisherInfo> publisherInfos = new ConcurrentHashMap<>();
    private final ScheduledExecutorService publishScheduler = Executors.newSingleThreadScheduledExecutor(ThingsBoardThreadFactory.forName("publish-scheduler"));

    @Value("${test-run.publisher-warmup-count:0}")
    private int publisherWarmUpCount;
    @Value("${test-run.publisher-warmup-iteration-sleep:0}")
    private int publisherWarmUpIterationSleep;
    @Value("${test-run.publisher-warmup-wait-time}")
    private int warmupWaitTime;
    @Value("${stats.enabled:true}")
    private boolean statsEnabled;
    @Value("${test-run.max_total_clients_per_iteration:0}")
    private int maxTotalClientsPerIteration;
    @Value("${test-run.max_publish_topic_group_idx}")
    private int maxPublishTopicGroupIdx;
    @Value("${test-run.publish-chunk-size-divider:1}")
    private int chunkSizeDivider;

    private boolean stopped = false;

    @Override
    public void connectPublishers() {
        List<PreConnectedPublisherInfo> preConnectedPublisherInfos = new ArrayList<>();
        int currentPublisherId = 0;
        for (PublisherGroup publisherGroup : testRunConfiguration.getPublishersConfig()) {
            int publisherTopicSuffixIdx = 0;
            for (int i = 0; i < publisherGroup.getPublishers(); i++) {
                if (publisherTopicSuffixIdx == maxPublishTopicGroupIdx) {
                    publisherTopicSuffixIdx = 0;
                }
                if (currentPublisherId++ % testRunClusterConfig.getParallelTestsCount() == testRunClusterConfig.getSequentialNumber()) {
                    preConnectedPublisherInfos.add(new PreConnectedPublisherInfo(publisherGroup, i, publisherTopicSuffixIdx));
                }
                publisherTopicSuffixIdx++;
            }
        }
        clusterProcessService.process("PUBLISHERS_CONNECT", preConnectedPublisherInfos, (latch, preConnectedPublisherInfo) -> {
            PublisherGroup publisherGroup = preConnectedPublisherInfo.getPublisherGroup();
            int publisherIndex = preConnectedPublisherInfo.getPublisherIndex();
            String clientId = clientIdService.createPublisherClientId(publisherGroup, publisherIndex);
            String topic = publisherGroup.getTopicPrefix() + preConnectedPublisherInfo.getPublisherTopicSuffix();
            MqttClient pubClient = clientInitializer.createClient(clientId, MqttPerformanceTest.DEFAULT_USER_NAME);
            clientInitializer.connectClient(CallbackUtil.createConnectCallback(
                            connectResult -> {
                                publisherInfos.put(clientId, new PublisherInfo(pubClient, clientId, topic,
                                        publisherGroup.isDebugEnabled() ? new DescriptiveStatistics() : null));
                                latch.countDown();
                            }, t -> {
                                log.warn("[{}] Failed to connect publisher", clientId);
                                pubClient.disconnectAndClose();
                                latch.countDown();
                            }
                    ),
                    pubClient);
        });
    }

    @Override
    public void warmUpPublishers() throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        CountDownLatch warmupCDL = new CountDownLatch(publisherInfos.size() * publisherWarmUpCount);
        AtomicBoolean successfulWarmUp = new AtomicBoolean(true);
        for (int i = 0; i < publisherWarmUpCount; i++) {
            log.info("Starting {} iteration of warmup!", i);
            AtomicInteger counter = new AtomicInteger(0);

            for (PublisherInfo publisherInfo : publisherInfos.values()) {
                try {
                    Message message = new Message(System.currentTimeMillis(), payloadGenerator.generatePayload());
                    publisherInfo.getPublisher().publish(publisherInfo.getTopic(), toByteBuf(mapper.writeValueAsBytes(message)),
                            CallbackUtil.createCallback(
                                    warmupCDL::countDown,
                                    t -> {
                                        successfulWarmUp.getAndSet(false);
                                        log.error("[{}] Error acknowledging warmup msg", publisherInfo.getClientId(), t);
                                        warmupCDL.countDown();
                                    }),
                            testRunConfiguration.getPublisherQoS());
                    int currentCounter = counter.incrementAndGet();
                    if (maxTotalClientsPerIteration > 0 && currentCounter == maxTotalClientsPerIteration) {
                        log.info("Reached {} counter of warmup! Sleeping for {}ms", maxTotalClientsPerIteration, publisherWarmUpIterationSleep);
                        Thread.sleep(publisherWarmUpIterationSleep);
                        counter.set(0);
                    }
                } catch (Exception e) {
                    log.error("[{}] Failed to publish", publisherInfo.getClientId(), e);
                    throw e;
                }
            }
        }

        boolean successfulWait = warmupCDL.await(warmupWaitTime, TimeUnit.SECONDS);
        if (!successfulWait || !successfulWarmUp.get()) {
            throw new RuntimeException("Failed to warm up publishers. " + warmupCDL.getCount() + " publishers couldn't acknowledge a message");
        }

        stopWatch.stop();
        log.info("Warming up {} publishers took {} ms.", publisherInfos.size(), stopWatch.getTime());
    }

    @Override
    public PublishStats startPublishing() {
        final Iterator<PublisherInfo> publisherInfoIterator = Iterables.cycle(publisherInfos.values()).iterator();
        DescriptiveStatistics publishSentLatencyStats = new DescriptiveStatistics();
        DescriptiveStatistics publishAcknowledgedStats = new DescriptiveStatistics();
        int publishPeriodMs = 1000 / testRunConfiguration.getMaxMessagesPerPublisherPerSecond();
        AtomicLong lastPublishTickTime = new AtomicLong(System.currentTimeMillis());

        int shortenedPublishPeriodMs = publishPeriodMs / chunkSizeDivider;

        int chunkSize;
        if (maxTotalClientsPerIteration > 0) {
            chunkSize = maxTotalClientsPerIteration / chunkSizeDivider;
        } else {
            chunkSize = publisherInfos.size() / chunkSizeDivider;
        }
        log.info("Chunk size is {}", chunkSize);

        int minChunkSize = Math.max(1, chunkSize / 7); // Ensure at least 1 message
        log.info("Load profile: Random between {} and {} messages per tick", minChunkSize, chunkSize);

        publishScheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            long actualPublishTickPause = now - lastPublishTickTime.getAndSet(now);
            if (actualPublishTickPause > publishPeriodMs * 1.5) {
                if (log.isDebugEnabled()) {
                    log.debug("Pause too long: expected {} ms, actual {} ms", publishPeriodMs, actualPublishTickPause);
                }
            }
            int currentTickChunkSize = ThreadLocalRandom.current().nextInt(minChunkSize, chunkSize + 1);
            for (int i = 0; i < currentTickChunkSize; i++) {
                process(publishSentLatencyStats, publishAcknowledgedStats, publisherInfoIterator.next());
            }
        }, 0, shortenedPublishPeriodMs, TimeUnit.MILLISECONDS);
        return new PublishStats(publishSentLatencyStats, publishAcknowledgedStats);
    }

    private void process(DescriptiveStatistics publishSentLatencyStats, DescriptiveStatistics publishAcknowledgedStats, PublisherInfo publisherInfo) {
        try {
            String topic;
            byte[] dataToSend;
            Message message = new Message(System.currentTimeMillis(), null);

            if (ThreadLocalRandom.current().nextDouble() < 0.4) {
                topic = TopicDictionary.getRandomPublishTopic(publisherInfo.getClientId());
                dataToSend = SmartPayloadGenerator.generatePayload(topic);
            } else {
                topic = publisherInfo.getTopic();
                byte[] payload = payloadGenerator.generatePayload();
                message.setPayload(payload);
                dataToSend = mapper.writeValueAsBytes(message);
            }

            ChannelFuture publishSentFuture = publisherInfo.getPublisher().publish(topic, toByteBuf(dataToSend),
                    CallbackUtil.createCallback(
                            () -> {
                                long ackLatency = System.currentTimeMillis() - message.getCreateTime();
                                if (statsEnabled) {
                                    publishAcknowledgedStats.addValue(ackLatency);
                                }
                                if (publisherInfo.isDebug()) {
                                    publisherInfo.getAcknowledgeLatencyStats().addValue(ackLatency);
                                    log.debug("[{}] Acknowledged msg with time {}", publisherInfo.getClientId(), message.getCreateTime());
                                }
                            },
                            t -> log.error("[{}] Failed to send msg.", publisherInfo.getClientId(), t)
                    ),
                    MqttQoS.valueOf(ThreadLocalRandom.current().nextInt(0, 3)));
            publishSentFuture
                    .addListener(future -> {
                                if (!future.isSuccess()) {
                                    log.debug("[{}] Error sending msg.", publisherInfo.getClientId(), future.cause());
                                } else {
                                    if (statsEnabled) {
                                        publishSentLatencyStats.addValue(System.currentTimeMillis() - message.getCreateTime());
                                    }
                                    if (publisherInfo.isDebug()) {
                                        log.debug("[{}] Sent msg with time {}", publisherInfo.getClientId(), message.getCreateTime());
                                    }
                                }
                            }
                    );
        } catch (Exception e) {
            log.error("[{}] Failed to publish", publisherInfo.getClientId(), e);
        }
    }

    @Override
    public void disconnectPublishers() {
        log.info("Disconnecting publishers.");
        publishScheduler.shutdownNow();
        for (PublisherInfo publisherInfo : publisherInfos.values()) {
            try {
                publisherInfo.getPublisher().disconnectAndClose();
            } catch (Exception e) {
                log.error("[{}] Failed to disconnect publisher", publisherInfo.getClientId());
            }
        }
        stopped = true;
    }

    @Override
    public void printDebugPublishersStats() {
        for (PublisherInfo publisherInfo : publisherInfos.values()) {
            DescriptiveStatistics stats = publisherInfo.getAcknowledgeLatencyStats();
            if (stats != null) {
                log.info("[{}] Publish acknowledge latency: messages - {}, median - {}, 95 percentile - {}, max - {}.",
                        publisherInfo.getClientId(), stats.getN(), stats.getMean(), stats.getPercentile(95), stats.getMax());
            }
        }
    }

    private static ByteBuf toByteBuf(byte[] bytes) {
        return Unpooled.wrappedBuffer(bytes);
    }

    @Getter
    @AllArgsConstructor
    private static class PreConnectedPublisherInfo {
        private final PublisherGroup publisherGroup;
        private final int publisherIndex;
        private final int publisherTopicSuffix;
    }

    @PreDestroy
    public void destroy() {
        if (!stopped) {
            log.info("Disconnecting publisher clients on destroy!");
            disconnectPublishers();
        }
    }

}
