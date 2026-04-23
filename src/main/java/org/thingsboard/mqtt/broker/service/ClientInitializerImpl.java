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
package org.thingsboard.mqtt.broker.service;

import com.google.common.collect.Iterables;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.mqtt.MqttVersion;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.thingsboard.mqtt.broker.client.mqtt.ConnectCallback;
import org.thingsboard.mqtt.broker.client.mqtt.MqttClient;
import org.thingsboard.mqtt.broker.client.mqtt.MqttClientConfig;
import org.thingsboard.mqtt.broker.client.mqtt.MqttHandler;
import org.thingsboard.mqtt.broker.client.mqtt.ReceivedMsgProcessor;
import org.thingsboard.mqtt.broker.data.dto.HostPortDto;
import org.thingsboard.mqtt.broker.tests.MqttPerformanceTest;
import org.thingsboard.mqtt.broker.util.ThingsBoardThreadFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClientInitializerImpl implements ClientInitializer {

    private final HostPortService hostPortService;
    private final SslConfig sslConfig;
    private final ReceivedMsgProcessor receivedMsgProcessor;

    @Value("${mqtt.netty.worker_group_thread_count:12}")
    private int groupThreadCount;
    @Value("${mqtt.client.keep-alive-seconds:600}")
    private int keepAliveSeconds;
    @Value("${mqtt.client.ip_addresses}")
    private String ipAddressStr;

    private EventLoopGroup eventLoopGroup;
    private Iterator<String> ipAddrIterator;

    @PostConstruct
    public void init() {
        eventLoopGroup = new NioEventLoopGroup(groupThreadCount, ThingsBoardThreadFactory.forName("just-nio-event-loop"));
        if (!StringUtils.isEmpty(ipAddressStr)) {
            List<String> ipAddrList = Arrays.asList(ipAddressStr.split(","));
            ipAddrIterator = Iterables.cycle(ipAddrList).iterator();
        }
    }

    @Override
    public MqttClient createClient(String clientId, String userName) {
        return createClient(clientId, userName, true, null);
    }

    @Override
    public MqttClient createClient(String clientId, String userName, boolean cleanSession) {
        return createClient(clientId, userName, cleanSession, null);
    }

    @Override
    public MqttClient createClient(String clientId, String userName, boolean cleanSession, MqttHandler defaultHandler) {
        MqttClientConfig config = new MqttClientConfig(sslConfig.getSslContext());
        config.setClientId(clientId);
        config.setUsername(MqttPerformanceTest.DEFAULT_USER_NAME);
        config.setPassword(MqttPerformanceTest.DEFAULT_PASSWORD);
        config.setCleanSession(cleanSession);
        config.setProtocolVersion(MqttVersion.MQTT_5);
        config.setTimeoutSeconds(keepAliveSeconds);
        MqttClient client = MqttClient.create(config, defaultHandler, receivedMsgProcessor, ipAddrIterator);
        client.setEventLoop(eventLoopGroup);
        return client;
    }

    @Override
    public void connectClient(ConnectCallback connectCallback, MqttClient client) {
        HostPortDto hostPort = hostPortService.getHostPort();
        client.connect(connectCallback, hostPort.getHost(), hostPort.getPort());
    }

    @PreDestroy
    public void destroy() {
        if (!eventLoopGroup.isShutdown()) {
            eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
    }
}
