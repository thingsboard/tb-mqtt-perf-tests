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
package org.thingsboard.mqtt.broker.service.orchestration;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import org.thingsboard.mqtt.broker.config.TestRunClusterConfig;
import org.thingsboard.mqtt.broker.controller.ClusterConst;
import org.thingsboard.mqtt.broker.data.NodeInfo;
import org.thingsboard.mqtt.broker.data.TestType;
import org.thingsboard.mqtt.broker.service.ServiceHelper;

@Slf4j
@Service
@RequiredArgsConstructor
public class TestRestServiceImpl implements TestRestService {

    private final TestRunClusterConfig testRunClusterConfig;
    private final RestTemplateBuilder restTemplateBuilder;
    @Autowired(required = false)
    private ServiceHelper serviceHelper;

    @Value("${test-run.orchestrator-url}")
    private String orchestratorUrl;
    @Value("${test-run.node-url}")
    private String nodeUrl;

    private RestTemplate restTemplate;
    String targetNodeUrl;

    @PostConstruct
    public void init() {
        this.restTemplate = restTemplateBuilder.build();

        String namespace = System.getenv("POD_NAMESPACE");
        if (!StringUtils.hasLength(namespace)) {
            namespace = "thingsboard-mqtt-broker";
        }

        if (StringUtils.isEmpty(nodeUrl) && serviceHelper != null) {
            log.info("nodeUrl is not set!");
            int id = serviceHelper.getId();
            TestType serviceHelperTestType = serviceHelper.getTestType();
            if (serviceHelperTestType == null) {
                return;
            }
            String testType = serviceHelperTestType.getPrintName();
            targetNodeUrl = "http://broker-tests-" + testType + "-" + id + ".broker-tests-" + testType + "." + namespace + ".svc.cluster.local:8088";
        } else {
            targetNodeUrl = nodeUrl;
        }
        log.info("Node URL: {}", targetNodeUrl);
    }

    @Override
    public void notifyClusterIsReady(String nodeUrl) {
        restTemplate.postForLocation(nodeUrl, "OK");
    }

    @Override
    public boolean notifyNodeIsReady() {
        if (StringUtils.isEmpty(orchestratorUrl) || StringUtils.isEmpty(targetNodeUrl)) {
            return false;
        }
        log.info("Notifying orchestrator about node readiness");
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(orchestratorUrl + ClusterConst.ORCHESTRATOR_PATH,
                    new NodeInfo(targetNodeUrl + ClusterConst.NODE_PATH, testRunClusterConfig.getParallelTestsCount()), String.class);
            return "OK".equals(response.getBody());
        } catch (Exception e) {
            log.error("Error notifying orchestrator", e);
            return false;
        }
    }
}
