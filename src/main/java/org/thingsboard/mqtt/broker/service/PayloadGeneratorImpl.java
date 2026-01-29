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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.mqtt.broker.config.TestRunConfiguration;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayloadGeneratorImpl implements PayloadGenerator {

    private final TestRunConfiguration testRunConfiguration;

    @Override
    public byte[] generatePayload() {
        List<String> telemetryKeys = testRunConfiguration.getTelemetryKeys();
        if (telemetryKeys == null || telemetryKeys.isEmpty()) {
            return generateRandomPayload(testRunConfiguration.getMinPayloadSize());
        }
        StringBuilder payloadBuilder = new StringBuilder();
        int i = 0;
        for (String key : telemetryKeys) {
            payloadBuilder.append(key).append(":").append(ThreadLocalRandom.current().nextInt(100));
            i++;
            if (i < telemetryKeys.size()) {
                payloadBuilder.append(",");
            }
        }
        byte[] telemetryBytes = payloadBuilder.toString().getBytes();
        if (telemetryBytes.length > testRunConfiguration.getMinPayloadSize()) {
            return telemetryBytes;
        }
        byte[] randomBytes = generateRandomPayload(testRunConfiguration.getMinPayloadSize());
        System.arraycopy(telemetryBytes, 0, randomBytes, 0, telemetryBytes.length);
        return randomBytes;
    }

    private static byte[] generateRandomPayload(int size) {
        byte[] payload = new byte[size];
        ThreadLocalRandom.current().nextBytes(payload);
        return payload;
    }
}
