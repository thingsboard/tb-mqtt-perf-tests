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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.mqtt.MqttQoS;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import org.thingsboard.mqtt.broker.data.PublisherGroup;
import org.thingsboard.mqtt.broker.data.SubscriberGroup;

import java.io.File;
import java.util.List;

@Component
@RequiredArgsConstructor
@ConditionalOnExpression("'${test-run.configuration-file:}'!=''")
public class FileTestRunConfiguration implements TestRunConfiguration {
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${test-run.configuration-file}")
    private String configurationFilePath;

    private File configurationFile;
    private TestRunConfigurationInfo testRunConfigurationInfo;

    @PostConstruct
    public void init() throws Exception {
        this.configurationFile = new File(configurationFilePath);
        this.testRunConfigurationInfo = mapper.readValue(configurationFile, TestRunConfigurationInfo.class);
    }

    @Override
    public String getConfigurationName() {
        return configurationFile.getName();
    }

    @Override
    public List<SubscriberGroup> getSubscribersConfig() {
        return testRunConfigurationInfo.getSubscriberGroups();
    }

    @Override
    public List<PublisherGroup> getPublishersConfig() {
        return testRunConfigurationInfo.getPublisherGroups();
    }

    @Override
    public int getMaxMessagesPerPublisherPerSecond() {
        return testRunConfigurationInfo.getMaxMsgsPerPublisherPerSecond();
    }

    @Override
    public int getSecondsToRun() {
        return testRunConfigurationInfo.getSecondsToRun();
    }

    @Override
    public int getAdditionalSecondsToWait() {
        return testRunConfigurationInfo.getAdditionalSecondsToWait();
    }

    @Override
    public int getTotalPublisherMessagesCount() {
        return getMaxMessagesPerPublisherPerSecond() * getSecondsToRun();
    }

    @Override
    public int getNumberOfDummyClients() {
        return testRunConfigurationInfo.getDummyClients();
    }

    @Override
    public MqttQoS getPublisherQoS() {
        return MqttQoS.valueOf(testRunConfigurationInfo.getPublisherQosValue());
    }

    @Override
    public MqttQoS getSubscriberQoS() {
        return MqttQoS.valueOf(testRunConfigurationInfo.getSubscriberQosValue());
    }

    @Override
    public int getMinPayloadSize() {
        return testRunConfigurationInfo.getMinPayloadSize();
    }

    @Override
    public List<String> getTelemetryKeys() {
        return testRunConfigurationInfo.getTelemetryKeys();
    }

    @Override
    public int getMaxConcurrentOperations() {
        return testRunConfigurationInfo.getMaxConcurrentOperations();
    }

}
