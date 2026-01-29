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
package org.thingsboard.mqtt.broker.data.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.thingsboard.mqtt.broker.data.BasicMqttCredentials;
import org.thingsboard.mqtt.broker.data.ClientCredentialsType;
import org.thingsboard.mqtt.broker.data.PersistentClientType;
import org.thingsboard.mqtt.broker.data.PubSubAuthorizationRules;
import org.thingsboard.mqtt.broker.util.JacksonUtil;

import java.util.List;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class MqttClientCredentialsDto {

    private String credentialsValue;
    private String name;
    private PersistentClientType clientType;
    private ClientCredentialsType credentialsType;

    public MqttClientCredentialsDto(String clientId, String name, PersistentClientType type, ClientCredentialsType credentialsType) {
        this.credentialsValue = JacksonUtil.toString(clientId == null ? newBasicCredentials() : newBasicCredentials(clientId));
        this.name = name;
        this.clientType = type;
        this.credentialsType = credentialsType;
    }

    private BasicMqttCredentials newBasicCredentials(String clientId) {
        return new BasicMqttCredentials(clientId, null, null, PubSubAuthorizationRules.newInstance(List.of(".*")));
    }

    private BasicMqttCredentials newBasicCredentials() {
        return new BasicMqttCredentials(null, "default", null, PubSubAuthorizationRules.newInstance(List.of(".*")));
    }
}
