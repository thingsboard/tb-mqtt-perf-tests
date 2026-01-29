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
package org.thingsboard.mqtt.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

/**
 * This simple class is used to generate configuration test file
 */
@Slf4j
public class TestConfigApp {

    private static final String SRC_DIR = "src";
    private static final String MAIN_DIR = "main";
    private static final String RESOURCES_DIR = "resources";
    private static final String FILE_NAME = "test.json";

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode result = createConfig(mapper);

        addGeneralConfig(mapper, result);

        String workDir = System.getProperty("user.dir");
        mapper.writerWithDefaultPrettyPrinter()
                .writeValue(Paths.get(workDir, SRC_DIR, MAIN_DIR, RESOURCES_DIR, FILE_NAME).toFile(), result);
    }

    private static void addGeneralConfig(ObjectMapper mapper, ObjectNode result) {
        result.put("dummyClients", 0);
        result.put("secondsToRun", 300);
        result.put("additionalSecondsToWait", 5);
        result.put("maxMsgsPerPublisherPerSecond", 1);
        result.put("publisherQosValue", 1);
        result.put("subscriberQosValue", 1);
        result.put("minPayloadSize", 1);
        result.put("maxConcurrentOperations", 3000);
        ArrayNode telemetryKeys = mapper.createArrayNode().add("lat").add("long").add("speed").add("fuel").add("batLvl");
        result.set("telemetryKeys", telemetryKeys);
    }

    private static ObjectNode createConfig(ObjectMapper mapper) {
        ObjectNode result = mapper.createObjectNode();

        ArrayNode subscriberGroups = mapper.createArrayNode();
        ArrayNode publisherGroups = mapper.createArrayNode();

        Set<String> uniqueRandomsSet = new HashSet<>();
        for (int i = 1; i <= 500; i++) {
            String random = RandomStringUtils.randomAlphabetic(5).toLowerCase();
            uniqueRandomsSet.add(random);

            ObjectNode subscriberGroup = mapper.createObjectNode();

            subscriberGroup.put("id", i);
            subscriberGroup.put("subscribers", 1);
            subscriberGroup.put("topicFilter", "usa/" + random + "/" + i + "/+");
            ArrayNode expectedPublisherGroups = mapper.createArrayNode();
            expectedPublisherGroups.add(i);
            subscriberGroup.set("expectedPublisherGroups", expectedPublisherGroups);
            ObjectNode persistentSessionInfo = mapper.createObjectNode();
            persistentSessionInfo.put("clientType", "APPLICATION");
            subscriberGroup.set("persistentSessionInfo", persistentSessionInfo);
            subscriberGroup.set("clientIdPrefix", null);

            subscriberGroups.add(subscriberGroup);

            ObjectNode publisherGroup = mapper.createObjectNode();

            publisherGroup.put("id", i);
            publisherGroup.put("publishers", 200000);
            publisherGroup.put("topicPrefix", "usa/" + random + "/" + i + "/");
            publisherGroup.set("clientIdPrefix", null);

            publisherGroups.add(publisherGroup);
        }
        System.out.println("Set size: " + uniqueRandomsSet.size());
        result.set("publisherGroups", publisherGroups);
        result.set("subscriberGroups", subscriberGroups);
        return result;
    }

}
