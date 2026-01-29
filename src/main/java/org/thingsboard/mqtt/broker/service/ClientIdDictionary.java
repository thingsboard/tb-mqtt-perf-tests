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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class ClientIdDictionary {

    private static final List<String> SMART_HOME_PREFIXES = Arrays.asList(
            "home-thermostat", "smart-bulb", "kitchen-fridge", "garage-opener", "security-cam", "alexa-hub"
    );

    private static final List<String> INDUSTRIAL_PREFIXES = Arrays.asList(
            "plc-siemens", "robot-arm", "conveyor-belt", "press-machine", "sensor-vibration", "scada-gateway"
    );

    private static final List<String> FLEET_PREFIXES = Arrays.asList(
            "tesla-truck", "delivery-drone", "cargo-ship", "taxi-fleet", "bus-shuttle"
    );

    private static final List<String> IT_PREFIXES = Arrays.asList(
            "k8s-worker", "aws-lambda", "db-replica", "load-balancer", "admin-console"
    );

    private static final List<String> LOCATIONS = Arrays.asList(
            "us-east", "eu-central", "asia-pac", "factory-01", "warehouse-B"
    );

    /**
     * Generates a realistic, readable Client ID.
     * Examples: "home-thermostat-8821", "k8s-worker-us-east-AF32", "robot-arm-line1"
     */
    public static String generateRandomClientId() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int category = random.nextInt(0, 5); // 0-4

        String prefix;
        String suffix = String.valueOf(random.nextInt(1000, 9999));

        switch (category) {
            case 0: // Smart Home (Friendly names)
                prefix = SMART_HOME_PREFIXES.get(random.nextInt(SMART_HOME_PREFIXES.size()));
                return prefix + "-" + suffix;

            case 1: // Industrial (Technical names)
                prefix = INDUSTRIAL_PREFIXES.get(random.nextInt(INDUSTRIAL_PREFIXES.size()));
                return prefix + "-line" + random.nextInt(1, 10) + "-" + suffix;

            case 2: // Fleet (Geo-tagged)
                prefix = FLEET_PREFIXES.get(random.nextInt(FLEET_PREFIXES.size()));
                return prefix + "-" + LOCATIONS.get(random.nextInt(LOCATIONS.size())) + "-" + suffix;

            case 3: // IT Infrastructure (Hex/UUID segments)
                prefix = IT_PREFIXES.get(random.nextInt(IT_PREFIXES.size()));
                String hex = Integer.toHexString(random.nextInt()).toUpperCase();
                // Take just first 4 chars of hex for readability
                if (hex.length() > 4) hex = hex.substring(0, 4);
                return prefix + "-" + hex;

            default: // Fallback / Generic UUID variant
                return "device-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }
}
