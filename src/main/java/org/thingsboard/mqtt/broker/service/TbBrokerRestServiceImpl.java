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

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.thingsboard.mqtt.broker.data.PageData;
import org.thingsboard.mqtt.broker.data.ShortMqttClientCredentials;
import org.thingsboard.mqtt.broker.data.dto.LoginDto;
import org.thingsboard.mqtt.broker.data.dto.LoginResponseDto;
import org.thingsboard.mqtt.broker.data.dto.MqttClientCredentialsDto;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnExpression("'${broker.type:}'=='THINGSBOARD'")
public class TbBrokerRestServiceImpl implements TbBrokerRestService {

    private final RestTemplateBuilder restTemplateBuilder;
    private RestTemplate restTemplate;

    @Value("${tb-broker.uri}")
    private String tbUri;
    @Value("${tb-broker.admin.username}")
    private String username;
    @Value("${tb-broker.admin.password}")
    private String password;

    @PostConstruct
    public void init() {
        this.restTemplate = restTemplateBuilder.build();

        LoginResponseDto loginResponse = restTemplate.postForEntity(tbUri + "/api/auth/login", new LoginDto(username, password), LoginResponseDto.class).getBody();
        if (loginResponse == null) {
           return;
        }

        log.info("Successfully logged into the ThingsBoard MQTT Broker.");
        this.restTemplate = restTemplateBuilder.defaultHeader("X-Authorization", "Bearer " + loginResponse.getToken()).build();
    }

    @Override
    public List<ShortMqttClientCredentials> getAllClientCredentials() {
        List<ShortMqttClientCredentials> clientCredentials = new ArrayList<>();
        boolean hasNext;
        int currentPage = 0;
        int pageSize = 50;
        do {
            PageData<ShortMqttClientCredentials> pageData = restTemplate.exchange(tbUri + "/api/mqtt/client/credentials?pageSize=" + pageSize + "&page=" + currentPage,
                    HttpMethod.GET, null,
                    new ParameterizedTypeReference<PageData<ShortMqttClientCredentials>>() {
                    },
                    Map.of()
            ).getBody();
            hasNext = pageData.hasNext();
            clientCredentials.addAll(pageData.getData());
            currentPage++;
        } while (hasNext);
        return clientCredentials;
    }

    @Override
    public String createClientCredentials(MqttClientCredentialsDto clientCredentialsDto) {
        JsonNode savedClientDto = restTemplate.postForEntity(tbUri + "/api/mqtt/client/credentials", clientCredentialsDto, JsonNode.class).getBody();
        if (savedClientDto == null) {
            throw new RuntimeException("Failed to save MQTT client credentials!");
        }
        return savedClientDto.get("id").asText();
    }

    @Override
    public void removeClientCredentials(UUID id) {
        restTemplate.delete(tbUri + "/api/mqtt/client/credentials/" + id.toString());
    }
}
