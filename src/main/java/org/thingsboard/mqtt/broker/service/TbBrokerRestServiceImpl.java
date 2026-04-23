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

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import org.thingsboard.mqtt.broker.data.ShortMqttClientCredentials;
import org.thingsboard.mqtt.broker.data.dto.MqttClientCredentialsDto;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@ConditionalOnExpression("'${broker.type:}'=='THINGSBOARD'")
public class TbBrokerRestServiceImpl implements TbBrokerRestService {

    @Override
    public List<ShortMqttClientCredentials> getAllClientCredentials() {
        return Collections.emptyList();
    }

    @Override
    public String createClientCredentials(MqttClientCredentialsDto clientCredentialsDto) {
        return UUID.randomUUID().toString();
    }

    @Override
    public void removeClientCredentials(UUID id) {
    }
}
