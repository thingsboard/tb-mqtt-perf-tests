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

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.mqtt.broker.util.ThingsBoardThreadFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

@Slf4j
@Service
public class ReceivedMsgProcessorImpl implements ReceivedMsgProcessor {
    private static final long MAX_DELAY = 50L;

    private final long logDelay;
    private final List<MsgHandler> handlers;
    private final ScheduledExecutorService logScheduler = Executors.newSingleThreadScheduledExecutor(ThingsBoardThreadFactory.forName("log-scheduler"));

    public ReceivedMsgProcessorImpl(@Value("${test-run.logs.msg-processing-queue-period}") long logDelay,
                                    @Value("${test-run.receive.threads:0}") int threads) {
        this.logDelay = logDelay;
        int handlersCount = threads > 0 ? threads : Runtime.getRuntime().availableProcessors();
        // built up front so that routing never sees an empty list; the threads start in init()
        this.handlers = Stream.generate(MsgHandler::new).limit(handlersCount).toList();
    }

    @PostConstruct
    public void init() {
        handlers.forEach(MsgHandler::start);
        log.info("Started {} message receive handlers", handlers.size());
        logScheduler.scheduleWithFixedDelay(() -> {
            log.info("Length of processing queue - {}", totalQueueSize());
        }, logDelay, logDelay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void processIncomingMessage(String clientId, MqttPublishMessage message, BiConsumer<MqttPublishMessage, Long> msgProcessor) {
        // one subscriber is always served by the same handler, so its messages stay in order
        handlers.get(Math.floorMod(clientId.hashCode(), handlers.size()))
                .add(new ReceivedMessage(message, System.currentTimeMillis(), msgProcessor));
    }

    private int totalQueueSize() {
        int size = 0;
        for (MsgHandler handler : handlers) {
            size += handler.queueSize();
        }
        return size;
    }

    @PreDestroy
    public void destroy() {
        log.info("Stopping {}, size of the queue - {}", ReceivedMsgProcessor.class.getSimpleName(), totalQueueSize());
        handlers.forEach(MsgHandler::stop);
        logScheduler.shutdownNow();
    }

    private static class MsgHandler {
        private final BlockingQueue<ReceivedMessage> pendingMessagesQueue = new LinkedBlockingQueue<>();
        private final ExecutorService executor = Executors.newSingleThreadExecutor(ThingsBoardThreadFactory.forName("msg-receive-handler"));

        void start() {
            executor.execute(this::processPendingMessages);
        }

        void add(ReceivedMessage message) {
            pendingMessagesQueue.add(message);
        }

        int queueSize() {
            return pendingMessagesQueue.size();
        }

        void stop() {
            executor.shutdownNow();
        }

        private void processPendingMessages() {
            while (!Thread.interrupted()) {
                ReceivedMessage receivedMessage;
                try {
                    receivedMessage = pendingMessagesQueue.poll(MAX_DELAY, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.info("Queue polling was interrupted.");
                    break;
                }
                if (receivedMessage == null) {
                    continue;
                }
                try {
                    receivedMessage.getMsgProcessor().accept(receivedMessage.getMsg(), receivedMessage.getReceivedTime());
                } catch (Exception e) {
                    log.warn("Failed to process message", e);
                } finally {
                    receivedMessage.getMsg().payload().release();
                }
            }
        }
    }
}
