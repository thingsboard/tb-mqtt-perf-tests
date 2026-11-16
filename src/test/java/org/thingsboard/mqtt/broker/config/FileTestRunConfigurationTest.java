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
package org.thingsboard.mqtt.broker.config;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.thingsboard.mqtt.broker.data.PersistentClientType;
import org.thingsboard.mqtt.broker.data.PersistentSessionInfo;
import org.thingsboard.mqtt.broker.data.SubscriberGroup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FileTestRunConfigurationTest {

    // general_use_case.json has non-persistent, APPLICATION and DEVICE subscriber groups.
    private static final String GENERAL_USE_CASE = "src/main/conf/general_use_case.json";

    @Test
    void givenNoOverride_whenLoaded_thenClientTypesAreAsInTheFile() throws Exception {
        Map<Integer, PersistentClientType> clientTypes = loadClientTypes("");

        assertThat(clientTypes).containsEntry(1, null).containsEntry(2, null)
                .containsEntry(3, PersistentClientType.APPLICATION)
                .containsEntry(4, PersistentClientType.DEVICE).containsEntry(5, PersistentClientType.DEVICE);
    }

    @Test
    void givenDeviceOverride_whenLoaded_thenEveryPersistentGroupIsDeviceAndNonPersistentStayNonPersistent() throws Exception {
        Map<Integer, PersistentClientType> clientTypes = loadClientTypes("DEVICE");

        assertThat(clientTypes).containsEntry(1, null).containsEntry(2, null)
                .containsEntry(3, PersistentClientType.DEVICE)
                .containsEntry(4, PersistentClientType.DEVICE).containsEntry(5, PersistentClientType.DEVICE);
    }

    private static Map<Integer, PersistentClientType> loadClientTypes(String persistentClientType) throws Exception {
        FileTestRunConfiguration configuration = new FileTestRunConfiguration();
        ReflectionTestUtils.setField(configuration, "configurationFilePath", GENERAL_USE_CASE);
        ReflectionTestUtils.setField(configuration, "persistentClientType", persistentClientType);
        configuration.init();
        List<SubscriberGroup> groups = configuration.getSubscribersConfig();
        assertThat(groups).hasSize(5);
        // Collectors.toMap rejects null values, and a non-persistent group maps to null.
        Map<Integer, PersistentClientType> clientTypes = new HashMap<>();
        for (SubscriberGroup group : groups) {
            PersistentSessionInfo info = group.getPersistentSessionInfo();
            clientTypes.put(group.getId(), info != null ? info.getClientType() : null);
        }
        return clientTypes;
    }
}
