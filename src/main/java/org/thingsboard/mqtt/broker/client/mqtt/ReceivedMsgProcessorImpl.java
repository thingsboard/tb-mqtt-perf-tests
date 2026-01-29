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
package org.thingsboard.mqtt.broker.client.mqtt;

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.mqtt.broker.util.ThingsBoardThreadFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

@Slf4j
@Service
public class ReceivedMsgProcessorImpl implements ReceivedMsgProcessor {
    private static final long MAX_DELAY = 50L;

    @Value("${test-run.logs.msg-processing-queue-period}")
    private long logDelay;

    private final BlockingQueue<ReceivedMessage> pendingMessagesQueue = new LinkedBlockingQueue<>();

    private final ExecutorService handlerExecutor = Executors.newSingleThreadExecutor(ThingsBoardThreadFactory.forName("msg-receive-handler"));
    private final ScheduledExecutorService logScheduler = Executors.newSingleThreadScheduledExecutor(ThingsBoardThreadFactory.forName("log-scheduler"));

    @PostConstruct
    public void init() {
        handlerExecutor.execute(this::processPendingMessages);
        logScheduler.scheduleWithFixedDelay(() -> {
            log.info("Length of processing queue - {}", pendingMessagesQueue.size());
        }, logDelay, logDelay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void processIncomingMessage(MqttPublishMessage message, BiConsumer<MqttPublishMessage, Long> msgProcessor) {
        pendingMessagesQueue.add(new ReceivedMessage(message, System.currentTimeMillis(), msgProcessor));
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

    @PreDestroy
    public void destroy() {
        log.info("Stopping {}, size of the queue - {}", ReceivedMsgProcessor.class.getSimpleName(), pendingMessagesQueue.size());
        handlerExecutor.shutdownNow();
        logScheduler.shutdownNow();
    }
}
