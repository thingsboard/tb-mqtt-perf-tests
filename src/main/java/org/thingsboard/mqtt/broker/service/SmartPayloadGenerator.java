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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class SmartPayloadGenerator {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Generates a realistic JSON byte array based on the topic context.
     */
    public static byte[] generatePayload(String topic) {
        ObjectNode json = mapper.createObjectNode();
        long now = System.currentTimeMillis();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        // 1. SMART HOME (Temp, Humidity, Status)
        if (topic.contains("temp")) {
            json.put("value", String.format("%.1f", 20.0 + random.nextDouble() * 5)); // 20.0 - 25.0 C
            json.put("unit", "C");
        } else if (topic.contains("humidity")) {
            json.put("value", random.nextInt(40, 70)); // 40-70%
            json.put("unit", "%");
        } else if (topic.contains("status") || topic.contains("lights")) {
            json.put("state", random.nextBoolean() ? "ON" : "OFF");
            json.put("brightness", random.nextInt(0, 101));
        }

        // 2. INDUSTRIAL (Vibration, RPM, Pressure)
        else if (topic.contains("rpm")) {
            json.put("rpm", random.nextInt(1000, 3500));
            json.put("load_pct", random.nextInt(50, 95));
        } else if (topic.contains("vibration")) {
            json.put("x_axis", String.format("%.3f", random.nextDouble()));
            json.put("y_axis", String.format("%.3f", random.nextDouble()));
            json.put("alert", random.nextDouble() > 0.95); // 5% chance of alert
        } else if (topic.contains("pressure")) {
            json.put("bar", String.format("%.2f", 2.0 + random.nextDouble()));
        }

        // 3. FLEET (GPS, Speed, Cargo)
        else if (topic.contains("gps") || topic.contains("vehicles")) {
            // Simulate coordinates around a specific city (e.g., Berlin/Frankfurt)
            double lat = 50.11 + (random.nextDouble() - 0.5) * 0.1;
            double lon = 8.68 + (random.nextDouble() - 0.5) * 0.1;
            json.put("latitude", lat);
            json.put("longitude", lon);
            json.put("speed", random.nextInt(0, 120));
            json.put("heading", random.nextInt(0, 360));
        } else if (topic.contains("cargo")) {
            json.put("door_open", random.nextBoolean());
            json.put("temp_internal", String.format("%.1f", -5.0 + random.nextDouble() * 2));
        }

        // 4. SYSTEM / IT (CPU, RAM, Logs)
        else if (topic.contains("cpu")) {
            json.put("usage_pct", random.nextInt(10, 90));
            json.put("temp_core", random.nextInt(45, 75));
        } else if (topic.contains("memory")) {
            json.put("free_mb", random.nextInt(1024, 8192));
            json.put("swap_used", random.nextInt(0, 512));
        } else if (topic.contains("logs") || topic.contains("error")) {
            String[] levels = {"INFO", "WARN", "ERROR"};
            json.put("level", levels[random.nextInt(levels.length)]);
            json.put("message", "Service trace id: " + random.nextInt(99999));
            json.put("code", random.nextInt(400, 505));
        }

        // 5. GENERIC FALLBACK
        else {
            json.put("data", random.nextLong());
            json.put("active", true);
        }

        // Always add timestamp for time-series graphs
        json.put("ts", now);

        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
}
