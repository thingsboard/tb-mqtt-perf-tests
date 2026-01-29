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
package org.thingsboard.mqtt.broker.data;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.thingsboard.mqtt.broker.client.mqtt.MqttClient;

import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class SubscriberInfo {
    @Setter
    private MqttClient subscriber;
    private final int id;
    private final String clientId;
    private final AtomicInteger totalReceivedMsgs;
    private final SubscriberGroup subscriberGroup;
    private final DescriptiveStatistics latencyStats;

    public SubscriberInfo(MqttClient subscriber, int id, String clientId, AtomicInteger totalReceivedMsgs, SubscriberGroup subscriberGroup) {
        this(subscriber, id, clientId, totalReceivedMsgs, subscriberGroup, null);
    }

    public SubscriberInfo(MqttClient subscriber, int id, String clientId, AtomicInteger totalReceivedMsgs, SubscriberGroup subscriberGroup, DescriptiveStatistics latencyStats) {
        this.subscriber = subscriber;
        this.id = id;
        this.clientId = clientId;
        this.totalReceivedMsgs = totalReceivedMsgs;
        this.subscriberGroup = subscriberGroup;
        this.latencyStats = latencyStats;
    }
}