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

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.mqtt.broker.data.dto.HostPortDto;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Service
public class HostPortServiceImpl implements HostPortService {

    @Value("${mqtt.host}")
    private String mqttHost;
    @Value("${mqtt.ports}")
    private int[] mqttPorts;

    private String targetMqttHost;

    @PostConstruct
    public void init() {
        if (mqttPorts == null || mqttPorts.length == 0) {
            throw new RuntimeException("Not valid MQTT ports value");
        }
        try {
            String brokerIpAddresses = System.getenv("MQTT_BROKER_IP_ADDRESSES");
            if (StringUtils.isNotEmpty(brokerIpAddresses)) {
                List<String> brokerIpAddrList = Arrays.asList(brokerIpAddresses.split(","));
                log.info("Broker IP addresses: {}", brokerIpAddrList);

                String serviceId = System.getenv("TB_SERVICE_ID");
                int id = Integer.parseInt(serviceId.replaceAll("[^0-9]", ""));
                log.info("Service ID: {}; ID: {}", serviceId, id);

                int idx = id % brokerIpAddrList.size();
                targetMqttHost = brokerIpAddrList.get(idx);
                log.info("Index: {}. Target MQTT host: {}", idx, targetMqttHost);
            }
        } catch (Exception e) {
            log.error("Error occurred during getting target MQTT host!", e);
        }
    }

    @Override
    public HostPortDto getHostPort() {
        return new HostPortDto(getTargetHost(), mqttPorts[ThreadLocalRandom.current().nextInt(mqttPorts.length)]);
    }

    private String getTargetHost() {
        return targetMqttHost == null ? mqttHost : targetMqttHost;
    }
}
