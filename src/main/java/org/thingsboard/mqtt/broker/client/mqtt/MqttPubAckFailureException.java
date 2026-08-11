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
package org.thingsboard.mqtt.broker.client.mqtt;

import lombok.Getter;

/**
 * Thrown when the broker acknowledges a publish with an MQTT 5 error reason code (0x80 and above).
 * Such a publish was NOT accepted, so counting it as a successful acknowledgement overstates
 * throughput - most visibly under a broker-side quota, where 0x97 QUOTA_EXCEEDED is the expected
 * response to an in-spec publish.
 */
@Getter
public class MqttPubAckFailureException extends RuntimeException {

    private final byte reasonCode;

    public MqttPubAckFailureException(String packetType, byte reasonCode) {
        super(packetType + " reason code 0x" + String.format("%02X", reasonCode));
        this.reasonCode = reasonCode;
    }

    public boolean isQuotaExceeded() {
        return reasonCode == (byte) 0x97;
    }

}
