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

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.concurrent.atomic.AtomicLong;

@Getter
@AllArgsConstructor
public class PublishStats {
    private final DescriptiveStatistics publishSentLatencyStats;
    private final DescriptiveStatistics publishAcknowledgedStats;
    /**
     * Publishes the broker refused with 0x97 QUOTA_EXCEEDED. These are acknowledged but NOT accepted,
     * so they are excluded from {@link #publishAcknowledgedStats}. Cumulative for the whole run.
     */
    private final AtomicLong quotaExceededPublishes;
    /**
     * Publishes the broker refused with any other MQTT 5 error reason code. Cumulative for the whole run.
     */
    private final AtomicLong refusedPublishes;
}
