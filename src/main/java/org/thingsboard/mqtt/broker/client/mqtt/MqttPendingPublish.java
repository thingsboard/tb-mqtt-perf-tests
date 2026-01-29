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
package org.thingsboard.mqtt.broker.client.mqtt;

import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.thingsboard.mqtt.broker.util.BasicCallback;

import java.util.function.Consumer;

final class MqttPendingPublish {

    private final int messageId;
    private final BasicCallback callback;
    private final MqttQoS qos;

    private boolean sent = false;

    MqttPendingPublish(int messageId, BasicCallback callback, MqttQoS qos) {
        this.messageId = messageId;
        this.callback = callback;
        this.qos = qos;
    }

    int getMessageId() {
        return messageId;
    }

    BasicCallback getCallback() {
        return callback;
    }


    boolean isSent() {
        return sent;
    }

    void setSent(boolean sent) {
        this.sent = sent;
    }


    MqttQoS getQos() {
        return qos;
    }

    void startPublishRetransmissionTimer(EventLoop eventLoop, Consumer<Object> sendPacket) {
    }

    void onPubackReceived() {
    }

    void setPubrelMessage(MqttMessage pubrelMessage) {
    }

    void startPubrelRetransmissionTimer(EventLoop eventLoop, Consumer<Object> sendPacket) {
    }

    void onPubcompReceived() {
    }
}
