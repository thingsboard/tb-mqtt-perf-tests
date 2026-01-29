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
import java.util.concurrent.ThreadLocalRandom;

public class TopicDictionary {

    // A mix of exact topics, single-level wildcards (+), and multi-level wildcards (#)
    public static final List<String> PATTERNS = Arrays.asList(
            // --- SMART HOME (Consumer patterns) ---
            "home/livingroom/temperature",
            "home/kitchen/fridge/status",
            "home/+/temperature",           // Monitor all rooms
            "home/+/+/status",              // Monitor all device statuses
            "home/security/#",              // All security alerts
            "lights/+/brightness",

            // --- INDUSTRIAL / FACTORY (Hierarchy patterns) ---
            "factory/line1/machine1/vibration",
            "factory/line1/+/rpm",          // Monitor RPM of all machines on Line 1
            "factory/+/+/alerts",           // All alerts from any machine on any line
            "sensors/pressure/+",
            "telemetry/energy/voltage",

            // --- FLEET MANAGEMENT (Geo/ID patterns) ---
            "fleet/truck/+/gps",            // Track all trucks
            "fleet/truck/1050/cargo",       // Specific truck cargo
            "vehicles/RegionA/#",           // All data from Region A
            "traffic/city/center/ density",

            // --- IT / SYSTEM ADMIN (System patterns) ---
            "sys/cpu/usage",
            "sys/memory/free",
            "sys/+/load",                   // Load for all nodes
            "logs/error/+",                 // Subscribe to all error logs
            "audit/#",                      // Compliance auditing

            // --- COMMAND & CONTROL (RPC patterns) ---
            "cmd/broadcast/restart",        // Listen for global restart
            "cmd/fw_update/v2",             // Listen for firmware updates
            "rpc/request/+"                 // Listen for RPC requests
    );

    // Dynamic Templates: These require String.format() with a client ID or Random ID
    public static final List<String> TEMPLATES = Arrays.asList(
            "device/%s/request",            // User's own command channel
            "device/%s/attribute/+",        // Updates for this specific device
            "group/%s/command",             // Command for a specific group (e.g., 'lighting')
            "sensors/%s/telemetry",         // Telemetry for a specific sensor type
            "building/b1/room/%s/environment"
    );

    private static final List<String> PUBLISH_TEMPLATES = Arrays.asList(
            // Mapped to Smart Home
            "home/livingroom/temperature",
            "home/kitchen/fridge/status",
            "home/%s/temperature",          // Fills + with Room
            "home/%s/smartbulb/status",     // Fills + with Room
            "home/security/motion/detected",
            "lights/%s/brightness",         // Fills + with Group

            // Mapped to Industrial
            "factory/line1/machine1/vibration",
            "factory/line1/%s/rpm",         // Fills + with Machine
            "factory/line1/%s/alerts",      // Fills + with Machine
            "sensors/pressure/%s",          // Fills + with SensorType
            "telemetry/energy/voltage",

            // Mapped to Fleet
            "fleet/truck/%s/gps",           // Fills + with Truck ID (using Room/Random)
            "fleet/truck/1050/cargo",
            "vehicles/RegionA/bus/001",
            "traffic/city/center/density",

            // Mapped to System
            "sys/cpu/usage",
            "sys/memory/free",
            "sys/node%s/load",              // Fills + with Node ID
            "logs/error/%s",                // Fills + with Service Name
            "audit/login/failure",

            // Mapped to RPC/General
            "cmd/broadcast/restart",
            "cmd/fw_update/v2",
            "rpc/request/%s"                // Fills + with Request ID
    );

    // Helper groups for randomization
    private static final List<String> GROUPS = Arrays.asList("lighting", "hvac", "security", "access_control");
    private static final List<String> SENSOR_TYPES = Arrays.asList("temp", "humidity", "co2", "motion", "pressure");
    private static final List<String> ROOMS = Arrays.asList("livingroom", "kitchen", "bedroom", "101", "102", "ServerRoom", "Lobby");
    private static final List<String> MACHINES = Arrays.asList("machine1", "machine2", "press", "conveyor");
    private static final List<String> SERVICES = Arrays.asList("backend", "frontend", "db", "auth");

    /**

     /**
     * Generates a realistic subscription topic.
     * @param myClientId The ID of the dummy client (used for point-to-point topics)
     * @return A valid MQTT topic string
     */
    public static String getRandomTopic(String myClientId) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        // 60% chance to pick a static/wildcard pattern
        if (random.nextDouble() < 0.6) {
            return PATTERNS.get(random.nextInt(PATTERNS.size()));
        }
        // 40% chance to generate a specific dynamic template
        else {
            String template = TEMPLATES.get(random.nextInt(TEMPLATES.size()));

            // Fill the template based on what it looks like
            if (template.contains("group")) {
                return String.format(template, GROUPS.get(random.nextInt(GROUPS.size())));
            } else if (template.contains("sensors")) {
                return String.format(template, SENSOR_TYPES.get(random.nextInt(SENSOR_TYPES.size())));
            } else if (template.contains("room")) {
                return String.format(template, ROOMS.get(random.nextInt(ROOMS.size())));
            } else {
                // Default: Use my own Client ID (simulating listening to my own commands)
                return String.format(template, myClientId);
            }
        }
    }

    /**
     * Generates a CONCRETE topic for publishing (No wildcards).
     * @param myClientId The ID of the publisher
     */
    public static String getRandomPublishTopic(String myClientId) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        // Pick a template designed for concrete publishing
        String template = PUBLISH_TEMPLATES.get(random.nextInt(PUBLISH_TEMPLATES.size()));

        // Dynamic Fill Logic
        // We use string.contains simply to guess which filler list to use

        if (template.contains("%s")) {
            // Smart Home / Rooms
            if (template.startsWith("home/") || template.contains("room")) {
                return String.format(template, ROOMS.get(random.nextInt(ROOMS.size())));
            }
            // Industrial Machines
            if (template.contains("factory") || template.contains("rpm") || template.contains("machine")) {
                return String.format(template, MACHINES.get(random.nextInt(MACHINES.size())));
            }
            // System / Logs
            if (template.contains("sys") || template.contains("logs")) {
                return String.format(template, SERVICES.get(random.nextInt(SERVICES.size())));
            }
            // Sensors
            if (template.contains("sensors")) {
                return String.format(template, SENSOR_TYPES.get(random.nextInt(SENSOR_TYPES.size())));
            }
            // Lights/Groups
            if (template.contains("lights")) {
                return String.format(template, GROUPS.get(random.nextInt(GROUPS.size())));
            }
            // Fleet / Generic
            // Default fallback: use a random number or client ID
            return String.format(template, random.nextInt(1000, 9999));
        }

        // If no %s, it's a static topic (e.g. "sys/cpu/usage")
        return template;
    }
}
